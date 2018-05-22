<?php

namespace robopoint\Controller\Api\V1;

use Aws\Kinesis\KinesisClient;
use Psr\Log\LoggerInterface;
use robocloud\Event\KinesisConsumerInMemoryErrorLogger;
use robocloud\Exception\InvalidMessageClassException;
use robocloud\Exception\InvalidMessageDataException;
use robocloud\Exception\ShardInitiationException;
use robocloud\Kinesis\Client\Consumer;
use robocloud\Kinesis\Client\Producer;
use robocloud\KinesisClientFactory;
use robocloud\Message\Message;
use robocloud\Message\MessageFactory;
use robocloud\Message\MessageInterface;
use robocloud\Message\MessageSchemaValidator;
use robocloud\MessageProcessing\Backend\KeepInMemoryBackend;
use robocloud\MessageProcessing\Filter\FilterByPurpose;
use robocloud\MessageProcessing\Filter\FilterByRoboId;
use robocloud\MessageProcessing\Filter\FilterInterface;
use robocloud\MessageProcessing\Processor\DefaultProcessor;
use robocloud\MessageProcessing\Transformer\KeepOriginalTransformer;
use robopoint\Kinesis\Client\RobopointConsumerRecovery;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;
use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

/**
 * @Route ("/api/v1/messages")
 */
class MessageController extends Controller
{

    /**
     * @var KeepInMemoryBackend
     */
    protected $backend;

    /**
     * @var KinesisConsumerInMemoryErrorLogger
     */
    protected $kinesisErrorHandler;

    /**
     * @var EventDispatcher
     */
    protected $eventDispatcher;

    /**
     * @var array
     */
    protected $config = [];

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * Millis behind latest.
     *
     * @var int
     */
    protected $lag;

    /**
     * @var array
     */
    protected $errors = [];

    /**
     * MessageController constructor.
     *
     * @param LoggerInterface $logger
     * @param ContainerInterface $container
     */
    public function __construct(LoggerInterface $logger, ContainerInterface $container)
    {

        $this->logger = $logger;
        $this->config = $container->getParameter('robopoint');

        $this->kinesisErrorHandler = new KinesisConsumerInMemoryErrorLogger();

        $this->eventDispatcher = new EventDispatcher();
        $this->eventDispatcher->addSubscriber($this->kinesisErrorHandler);
        $this->eventDispatcher->addSubscriber(new MessageSchemaValidator($this->config['message_schema_dir']));

        $this->backend = new KeepInMemoryBackend();
    }

    /**
     * @Route("/")
     * @Method("POST")
     *
     * @param Request $request
     *
     * @return JsonResponse
     */
    public function pushAction(Request $request)
    {

        $payload = $request->request->get('messages');

        if (empty($payload)) {
            return new JsonResponse([
                'errors' => ['The request does not contain the expected "messages" offset.'],
            ], 400);
        }

        $producer = new Producer(
            $this->getKinesisClient('producer'),
            $this->config['stream_name'],
            $this->getMessageFactory(),
            $this->getEventDispatcher()
        );

        $response = [];

        try {
            foreach ($payload as $data) {

                if (empty($data['version'])) {
                    $data['version'] = $this->config['message_schema_version'];
                }

                $message = $this->getMessageFactory()->setMessageData($data)->createMessage();
                $producer->add($message);
                $message_array = $message->jsonSerialize();
                $response[] = [
                    'messageId' => $message_array['messageId'],
                    'messageTime' => $message_array['messageTime'],
                ];
            }
        } catch (InvalidMessageDataException $e) {
            return new JsonResponse([
                'errors' => [
                    [
                        'message' => 'Invalid message data provided.',
                        'data' => [$data],
                    ],
                ],
            ], 400);
        } catch (InvalidMessageClassException $e) {

            $this->logger->error($e->getMessage());

            return new JsonResponse(['errors' => [
                ['message' => 'System error.'],
            ]], 500);
        } catch (\InvalidArgumentException $e) {

            $this->logger->error($e->getMessage());

            return new JsonResponse(['errors' => [
                ['message' => 'System error.'],
            ]], 500);
        }

        $producer->pushAll();

        if ($errors = $this->kinesisErrorHandler->getErrors()) {

            array_map(function ($error) {
                $this->logger->error($error['message'], ['exception' => $error['exception']]);
            }, $errors);

            return new JsonResponse(['errors' => [
                ['message' => 'System error.'],
            ]], 500);
        }

        return new JsonResponse([
            'messages' => $response,
        ]);
    }

    /**
     * @Route("/{roboId}/read-by-purpose/{purpose}")
     * @Method("GET")
     *
     * @param string $roboId
     * @param string $purpose
     *
     * @return JsonResponse
     */
    public function getActionReadByPurpose($roboId, $purpose)
    {
        $filter = new FilterByPurpose($purpose);
        $messages = $this->readMessages($roboId, $filter);
        return $this->getReadJsonResponse($messages);
    }

    /**
     * @Route("/{roboId}/read-from")
     * @Method("GET")
     *
     * @param string $roboId
     *
     * @return JsonResponse
     */
    public function getActionReadFrom($roboId)
    {
        $filter = new FilterByRoboId($roboId);
        $messages = $this->readMessages($roboId, $filter);
        return $this->getReadJsonResponse($messages);
    }

    /**
     * @Route("/{roboId}/read-by-purpose/{purpose}/last/get-property/{property}")
     * @Method("GET")
     *
     * @param string $roboId
     * @param string $purpose
     * @param string $property
     *
     * @return Response
     */
    public function getActionGetByPurposeLastByProperty($roboId, $purpose, $property) {
        $filter = new FilterByPurpose($purpose);

        do {
            $messages = $this->readMessages($roboId, $filter);
        }
        while ($this->getLag() > 0);

        $response = '';

        if (!empty($messages)) {
            $message = array_pop($messages);
            $data = $message->getData();
            if (!isset($data[$property])) {
                return new Response('The provided property "' . $property . '" was not found.', 400);
            }

            $response = $data[$property];
        }

        if (!empty($this->errors['critical'])) {
            return new Response('System error.', 500);
        }
        elseif (!empty($this->errors['user'])) {
            return new Response(implode(' | ', $this->errors['user']), 400);
        }

        return new Response($response);

    }

    /**
     * Gets consumer to load messages from Kinesis stream.
     *
     * @param string $roboId
     * @param FilterInterface $filter
     *
     * @return MessageInterface[]
     */
    protected function readMessages($roboId, FilterInterface $filter)
    {

        $transformer = new KeepOriginalTransformer();
        $this->getEventDispatcher()->addSubscriber(new DefaultProcessor($filter, $transformer, $this->getBackend()));
        $this->getEventDispatcher()->addSubscriber($this->kinesisErrorHandler);

        $consumer = new Consumer(
            $this->getKinesisClient('consumer'),
            $this->config['stream_name'],
            $this->getMessageFactory(),
            $this->getEventDispatcher(),
            new RobopointConsumerRecovery(
                $this->config['stream_name'],
                $this->config['kinesis']['consumer']['recovery_file'],
                $roboId,
                get_class($filter)
            )
        );

        try {
            $consumer->consume(0);
        }
        catch (ShardInitiationException $e) {
            $this->logger->critical($e->getMessage(), ['exception' => $e]);
            $this->errors['critical'][] = $e->getMessage();
        }

        if ($errors = $this->kinesisErrorHandler->getErrors()) {

            array_map(function ($error) {
                if (is_a($error['exception'], InvalidMessageDataException::class)) {
                    $this->logger->error($error['message'], ['exception' => $error['exception']]);
                    $this->errors['user'][] = $error['message'];
                }
                else {
                    $this->logger->critical($error['message'], ['exception' => $error['exception']]);
                    $this->errors['critical'][] = $error['message'];
                }
            }, $errors);
        }

        $this->lag = $consumer->getLag();

        return $this->getBackend()->flush();

    }

    /**
     * @param array $messages
     * @return JsonResponse
     */
    protected function getReadJsonResponse(array $messages) {
        if (!empty($this->errors['critical'])) {
            return new JsonResponse(['errors' => [
                ['message' => 'System error.'],
            ]], 500);
        }
        elseif (!empty($this->errors['user'])) {
            return new JsonResponse(['errors' => array_map(function($error) {
                return ['message' => $error];
            }, $this->errors['user'])], 400);
        }

        return new JsonResponse([
            'messages' => $messages,
            'lag' => $this->getLag(),
        ]);
    }

    /**
     * @return MessageFactory
     */
    public function getMessageFactory(): MessageFactory
    {
        return new MessageFactory(Message::class, $this->getEventDispatcher());
    }

    /**
     * @return EventDispatcher
     */
    public function getEventDispatcher(): EventDispatcher
    {
        return $this->eventDispatcher;
    }

    /**
     * @param $type
     * @return \Aws\Kinesis\KinesisClient
     */
    public function getKinesisClient($type): KinesisClient
    {
        $factory = new KinesisClientFactory($this->config['kinesis']['api_version'], $this->config['kinesis']['region']);
        if ($type == 'producer') {
            return $factory->getKinesisClient(
                $this->config['kinesis']['producer']['key'],
                $this->config['kinesis']['producer']['secret']
            );
        } elseif ($type == 'consumer') {
            return $factory->getKinesisClient(
                $this->config['kinesis']['consumer']['key'],
                $this->config['kinesis']['consumer']['secret']
            );
        }
    }

    /**
     * @return KeepInMemoryBackend
     */
    public function getBackend(): KeepInMemoryBackend
    {
        return $this->backend;
    }

    /**
     * @return KinesisConsumerInMemoryErrorLogger
     */
    public function getKinesisErrorHandler(): KinesisConsumerInMemoryErrorLogger
    {
        return $this->kinesisErrorHandler;
    }

    /**
     * Gets millis behind latest.
     *
     * @return int
     */
    public function getLag() {
        return $this->lag;
    }

}
