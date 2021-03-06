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
     * @var string
     */
    protected $roboId;

    /**
     * @var string
     */
    protected $streamName;

    /**
     * @var string
     */
    protected $consumerRecoveryFile;

    /**
     * @var string
     */
    protected $filter;

    /**
     * RobopointConsumerRecovery constructor.
     *
     * @param $stream_mame
     * @param $consumer_recovery_file
     * @param $robo_id
     * @param $filter
     */
    public function __construct($stream_mame, $consumer_recovery_file, $robo_id, $filter)
    {
        $this->streamName = $stream_mame;
        $this->consumerRecoveryFile = $consumer_recovery_file;
        $this->roboId = $robo_id;
        $this->filter = $filter;
    }

    /**
     * {@inheritdoc}
     */
    public function hasRecoveryData() {
        $content = $this->getRecoveryFileContent();
        return isset($content[$this->streamName][$this->roboId][$this->filter]);
    }

    /**
     * {@inheritdoc}
     */
    public function getLastSequenceNumber($shard_id) {
        $content = $this->getRecoveryFileContent();

        if (isset($content[$this->streamName][$this->roboId][$this->filter][$shard_id])) {
            return $content[$this->streamName][$this->roboId][$this->filter][$shard_id];
        }

        return NULL;
    }

    /**
     * {@inheritdoc}
     */
    public function storeLastSuccessPosition($shard_id, $sequence_number) {
        $content = $this->getRecoveryFileContent();
        $content[$this->streamName][$this->roboId][$this->filter][$shard_id] = $sequence_number;

        $write_result = file_put_contents($this->consumerRecoveryFile, json_encode($content));

        if ($write_result === FALSE) {
            throw new ConsumerRecoveryException('Error writing Consumer recovery data at ' . $this->consumerRecoveryFile);
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
        $content = file_get_contents($this->consumerRecoveryFile);
        if (!empty($content)) {
            return json_decode($content, TRUE);
        }
        elseif ($content === FALSE) {
            throw new ConsumerRecoveryException('Error reading Consumer recovery data at ' . $this->consumerRecoveryFile);
        }

        return [];
    }

}
