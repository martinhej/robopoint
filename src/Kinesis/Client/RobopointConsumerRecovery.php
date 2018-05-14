<?php

namespace robopoint\Kinesis\Client;

use robocloud\Config\ConfigInterface;
use robocloud\Exception\ConsumerRecoveryException;
use robocloud\Kinesis\Client\ConsumerRecoveryInterface;

/**
 * Class RobopointConsumerRecovery.
 *
 * @package robopoint\Kinesis\Client
 */
class RobopointConsumerRecovery implements ConsumerRecoveryInterface {

    /**
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @var string
     */
    protected $roboId;

    /**
     * @var string
     */
    protected $streamName;

    /**
     * RobopointConsumerRecovery constructor.
     * @param ConfigInterface $config
     * @param $robo_id
     */
    public function __construct(ConfigInterface $config, $robo_id) {
        $this->config = $config;
        $this->roboId = $robo_id;
        $this->streamName = $config->getStreamName();
    }

    /**
     * {@inheritdoc}
     */
    public function hasRecoveryData() {
        $content = $this->getRecoveryFileContent();
        return isset($content[$this->streamName][$this->roboId]);
    }

    /**
     * {@inheritdoc}
     */
    public function getLastSequenceNumber($shard_id) {
        $content = $this->getRecoveryFileContent();

        if (isset($content[$this->streamName][$this->roboId][$shard_id])) {
            return $content[$this->streamName][$this->roboId][$shard_id];
        }

        return NULL;
    }

    /**
     * {@inheritdoc}
     */
    public function storeLastSuccessPosition($shard_id, $sequence_number) {
        $content = $this->getRecoveryFileContent();
        $content[$this->streamName][$this->roboId][$shard_id] = $sequence_number;

        $write_result = file_put_contents($this->getConfig()->getRecoveryConsumerRecoveryFile(), json_encode($content));

        if ($write_result === FALSE) {
            throw new ConsumerRecoveryException('Error writing Consumer recovery data at ' . $this->getConfig()->getRecoveryConsumerRecoveryFile());
        }
    }

    /**
     * Gets the recovery file contents.
     *
     * @return array
     *   The recovery file content as array.
     *
     * @throws \robocloud\Exception\ConsumerRecoveryException
     *   On the recovery file read error.
     */
    protected function getRecoveryFileContent() {
        $content = file_get_contents($this->getConfig()->getRecoveryConsumerRecoveryFile());
        if (!empty($content)) {
            return json_decode($content, TRUE);
        }
        elseif ($content === FALSE) {
            throw new ConsumerRecoveryException('Error reading Consumer recovery data at ' . $this->getConfig()->getRecoveryConsumerRecoveryFile());
        }

        return [];
    }

    /**
     * @return ConfigInterface
     */
    public function getConfig() {
        return $this->config;
    }
}