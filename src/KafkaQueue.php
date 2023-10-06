<?php

namespace DiKay\Kafka;

use Carbon\Exceptions\Exception as CarbonException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\Log;
use Junges\Kafka\Config\Sasl;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
use Junges\Kafka\Exceptions\KafkaConsumerException;
use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Message\Message;

class KafkaQueue extends Queue implements QueueContract
{
    protected string $bootstrapServers;
    protected string $queue;
    protected Sasl $sasl;

    public function __construct(array $config)
    {
        $this->bootstrapServers = $config['bootstrap_servers'];
        $this->queue = $config['queue'];
        $this->sasl = new Sasl(
            $config['sasl_username'],
            $config['sasl_password'],
            $config['sasl_mechanism'],
            $config['security_protocol'],
        );
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        try {
            Kafka::publishOn($queue ?? $this->queue, $this->bootstrapServers)
                ->withSasl($this->sasl)
                ->withMessage(new Message($queue, body: serialize($job)))
                ->send();
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
            $consumer = Kafka::createConsumer([$queue ?? $this->queue], brokers: $this->bootstrapServers)
                ->withSasl($this->sasl)
                ->withHandler($this->jobHandler())
                ->build();

            $consumer->consume();
        } catch (CarbonException|KafkaConsumerException $e) {
            Log::error(self::class . ' ' . $e->getMessage());
        }
    }

    protected function jobHandler(): callable
    {
        return function(KafkaConsumerMessage $message) {
            $job = unserialize($message->getBody());
            $job->handle();

            $mes = is_string($message->getBody()) ? $message->getBody() : json_encode($message->getBody()) ;
            Log::info('In topic '. $message->getTopicName() .' consumed message: ' . $mes);
            echo "In topic {$message->getTopicName()} consumed message: $mes\n";
        };
    }
}
