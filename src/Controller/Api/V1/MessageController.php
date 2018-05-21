<?php

namespace robopoint\Controller\Api\V1;

use Aws\Kinesis\KinesisClient;
use Psr\Log\LoggerInterface;
use robocloud\Event\KinesisConsumerInMemoryErrorLogger;
use robocloud\Exception\InvalidMessageClassException;
use robocloud\Exception\InvalidMessageDataException;
use robocloud\Kinesis\Client\Consumer;
use robocloud\Kinesis\Client\Producer;
use robocloud\KinesisClientFactory;
use robocloud\Message\Message;
use robocloud\Message\MessageFactory;
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
        return $this->readMessages($roboId, $filter);
    }

    /**
     * @Route("/{roboId}/read-my")
     * @Method("GET")
     *
     * @param string $roboId
     *
     * @return JsonResponse
     */
    public function getActionReadMy($roboId)
    {
        $filter = new FilterByRoboId($roboId);
        return $this->readMessages($roboId, $filter);
    }

    /**
     * Gets consumer to load messages from Kinesis stream.
     *
     * @param string $roboId
     * @param FilterInterface $filter
     *
     * @return JsonResponse
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
        } catch (\Exception $e) {

            $this->container->error($e->getMessage(), $e);

            return new JsonResponse(['errors' => [
                ['message' => 'System error.'],
            ]], 500);
        }

        if ($errors = $this->kinesisErrorHandler->getErrors()) {

            array_map(function ($error) {
                $this->logger->error($error['message'], ['exception' => $error['exception']]);
            }, $errors);

            return new JsonResponse(['errors' => [
                ['message' => 'System error.'],
            ]], 500);
        }

        $messages = $this->getBackend()->flush();

        return new JsonResponse([
            'messages' => $messages,
            'lag' => $consumer->getLag(),
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

}
