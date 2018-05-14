<?php

namespace robopoint\Controller\Api\V1;

use robocloud\Config\DefaultConfig;
use robocloud\Event\KinesisConsumerInMemoryErrorLogger;
use robocloud\Kinesis\Client\Consumer;
use robocloud\Kinesis\Client\Producer;
use robocloud\KinesisClientFactory;
use robocloud\Message\Message;
use robocloud\Message\MessageFactory;
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
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

/**
 * @Route ("/api/v1/messages")
 */
class MessageController extends Controller {

    /**
     * @var KeepInMemoryBackend
     */
    protected $backend;

    /**
     * @var KinesisConsumerInMemoryErrorLogger
     */
    protected $errorLogger;

    /**
     * @var EventDispatcher
     */
    protected $eventDispatcher;

    protected $robocloudConfig;

    /**
     * MessageController constructor.
     */
    public function __construct() {
        $this->backend = new KeepInMemoryBackend();
        $this->errorLogger = new KinesisConsumerInMemoryErrorLogger();
        $this->eventDispatcher = new EventDispatcher();
        $this->robocloudConfig = new DefaultConfig(__DIR__ . '/../../../../config/robocloud.yaml');
    }

    /**
     * @Route("/")
     * @Method("POST")
     *
     * @param Request $request
     *
     * @return JsonResponse
     */
    public function pushAction(Request $request) {

        $payload = $request->request->get('messages');

        if (empty($payload)) {
            return new JsonResponse([
                'errors' => ['Empty payload'],
            ], 400);
        }
//        $payload = json_decode($payload);

        if (!is_array($payload)) {
            return new JsonResponse([
                'errors' => ['Malformed request data.'],
            ], 400);
        }

        $producer = new Producer(
            $this->getKinesisClient('producer'),
            $this->getRobocloudConfig()->getStreamName(),
            $this->getMessageFactory(),
            $this->getEventDispatcher()
        );

        try {
            foreach ($payload as $message) {
                $producer->add($this->getMessageFactory()->setMessageData($message)->createMessage());
            }
        }
        catch (\Exception $e) {
            return new JsonResponse([
                'errors' => ['Invalid message data provided'],
            ], 400);
        }

        $result = $producer->pushAll();

        if ($errors = $this->errorLogger->getErrors()) {
            return new JsonResponse(['errors' => array_map(function ($error) {
                return $error['message'];
            }, $errors)], 500);
        }

        return new JsonResponse([
            'received' => $result,
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
    public function getActionReadByPurpose($roboId, $purpose) {
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
    public function getActionReadMy($roboId) {
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
     *
     * @todo - add logging, provide client with meaningful messages.
     */
    protected function readMessages($roboId, FilterInterface $filter) {

        $transformer = new KeepOriginalTransformer();
        $this->getEventDispatcher()->addSubscriber(new DefaultProcessor($filter, $transformer, $this->getBackend()));
        $this->getEventDispatcher()->addSubscriber($this->errorLogger);

        $consumer = new Consumer(
            $this->getKinesisClient('consumer'),
            $this->getRobocloudConfig()->getStreamName(),
            $this->getMessageFactory(),
            $this->getEventDispatcher(),
            new RobopointConsumerRecovery($this->getRobocloudConfig(), $roboId)
        );

        try {
            $consumer->consume(0);
        }
        catch (\Exception $e) {
            return new JsonResponse(['errors' => [$e->getMessage()]], 500);
        }

        if ($errors = $this->errorLogger->getErrors()) {
            return new JsonResponse(['errors' => array_map(function ($error) {
                return $error['message'];
            }, $errors)], 500);
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
    public function getMessageFactory() {
        return new MessageFactory(Message::class, $this->getEventDispatcher());
    }

    public function getEventDispatcher() {
        return $this->eventDispatcher;
    }

    public function getKinesisClient($type) {
        $factory = new KinesisClientFactory($this->getRobocloudConfig());
        return $factory->getKinesisClient($type);
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
    public function getErrorLogger(): KinesisConsumerInMemoryErrorLogger
    {
        return $this->errorLogger;
    }

    /**
     * @return DefaultConfig
     */
    public function getRobocloudConfig(): DefaultConfig
    {
        return $this->robocloudConfig;
    }



}
