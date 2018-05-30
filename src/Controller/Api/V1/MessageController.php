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
use Symfony\Component\Cache\Simple\FilesystemCache;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Serializer\Encoder\CsvEncoder;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

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
     * @var FilesystemCache
     */
    protected $cache;

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
        $this->cache = new FilesystemCache();
    }

    /**
     * Endpoint to create messages.
     *
     * For the data structure to be sent see the robocloud/schema.
     *
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
            $this->getEventDispatcher(),
            $this->cache
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
     * Reads messages filtered by the purpose.
     *
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
        $messages = $this->readMessages($roboId, $filter, $purpose);
        return $this->getReadJsonResponse($messages);
    }

    /**
     * Gets the last message with the given purpose.
     *
     * @Route("/{roboId}/read-by-purpose/{purpose}/last")
     * @Method("GET")
     *
     * @param string $roboId
     * @param string $purpose
     *
     * @return JsonResponse
     */
    public function getActionReadByPurposeLast($roboId, $purpose)
    {
        $filter = new FilterByPurpose($purpose);

        do {
            $messages = $this->readMessages($roboId, $filter, $purpose);
        }
        while ($this->getLag() > 0);

        return $this->getReadJsonResponse($messages);
    }

    /**
     * Gets the last message in CSV format.
     *
     * @Route("/{roboId}/read-by-purpose/{purpose}/last/csv")
     * @Method("GET")
     *
     * @param string $roboId
     * @param string $purpose
     *
     * @return Response
     */
    public function getActionReadByPurposeLastCsv($roboId, $purpose)
    {
        $filter = new FilterByPurpose($purpose);

        do {
            $messages = $this->readMessages($roboId, $filter, $purpose);
        }
        while ($this->getLag() > 0);

        if (!empty($this->errors['critical'])) {
            return new Response('System error.', 500);
        }
        elseif (!empty($this->errors['user'])) {
            return new Response(implode(' | ', $this->errors['user']), 400);
        }

        $content = '';

        if (!empty($messages)) {
            $message = array_pop($messages);
            $serializer = new Serializer([new ObjectNormalizer()], [new CsvEncoder()]);
            $content = $serializer->encode($message->getData(), 'csv');
        }

        return new Response($content);
    }

    /**
     * Read messages from a specific robot.
     *
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
     * Gets message property value of the last message for given purpose.
     *
     * Endpoint meant for simple devices for which it is too complicated
     * to parse JSON and/or to traverse the messages structure.
     *
     * Note that this endpoint is able to return only simple string values
     * otherwise it will return 400 error.
     *
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
            $messages = $this->readMessages($roboId, $filter, $purpose);
        }
        while ($this->getLag() > 0);

        $content = '';

        if (!empty($messages)) {
            $message = array_pop($messages);
            $data = $message->getData();
            if (!isset($data[$property])) {
                return new Response('The provided property "' . $property . '" was not found.', 400);
            }

            if (!is_scalar($data[$property])) {
                return new Response('The requested structure cannot be output as simple string value', 400);
            }

            $content = $data[$property];
        }

        if (!empty($this->errors['critical'])) {
            return new Response('System error.', 500);
        }
        elseif (!empty($this->errors['user'])) {
            return new Response(implode(' | ', $this->errors['user']), 400);
        }

        return new Response($content);

    }

    /**
     * Reads messages from stream.
     *
     * @param string $roboId
     * @param FilterInterface $filter
     * @param string $consumer_recovery_offset
     *
     * @return MessageInterface[]
     */
    protected function readMessages($roboId, FilterInterface $filter, $consumer_recovery_offset = NULL)
    {

        if (empty($consumer_recovery_offset)) {
            $consumer_recovery_offset = get_class($filter);
        }

        $transformer = new KeepOriginalTransformer();
        $this->getEventDispatcher()->addSubscriber(new DefaultProcessor($filter, $transformer, $this->getBackend()));
        $this->getEventDispatcher()->addSubscriber($this->kinesisErrorHandler);

        $consumer = new Consumer(
            $this->getKinesisClient('consumer'),
            $this->config['stream_name'],
            $this->getMessageFactory(),
            $this->getEventDispatcher(),
            $this->cache,
            new RobopointConsumerRecovery(
                $this->config['stream_name'],
                $this->config['kinesis']['consumer']['recovery_file'],
                $roboId,
                $consumer_recovery_offset
            )
        );

        try {
            $consumer->consume(0, 2);
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
     * Composes a JSON response.
     *
     * @param array $messages
     * @return JsonResponse
     */
    protected function getReadJsonResponse(array $messages): JsonResponse {
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
