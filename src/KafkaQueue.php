<?php

namespace DiKay\Kafka;

use Carbon\Exceptions\Exception as CarbonException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\Log;
use Junges\Kafka\Contracts\CanConsumeMessages;
use Junges\Kafka\Contracts\CanProduceMessages;
use Junges\Kafka\Exceptions\KafkaConsumerException;
use Junges\Kafka\Message\Message;

class KafkaQueue extends Queue implements QueueContract
{
    public function __construct(
        protected readonly CanConsumeMessages $consumer,
        protected readonly CanProduceMessages $producer,
    ) {
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        try {
            $this->producer->withMessage(new Message(body: serialize($job)))->send();
        } catch (Exception $e) {
            Log::error(self::class . ' ' . $e->getMessage());
        }
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    public function pop($queue = null)
    {
        try {
            $this->consumer->consume();
        } catch (CarbonException|KafkaConsumerException $e) {
            Log::error(self::class . ' ' . $e->getMessage());
        }
    }
}
