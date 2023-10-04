<?php

namespace DiKay\Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Facades\Log;
use Junges\Kafka\Config\Sasl;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
use Junges\Kafka\Facades\Kafka;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $producer = Kafka::publishOn(config('kafka.topics')[0])
            ->withSasl(new Sasl(
                config('kafka.username'),
                config('kafka.password'),
                config('kafka.mechanism'),
                config('kafka.security_protocol'),
            ));

        $consumer = Kafka::createConsumer(config('kafka.topics'))
            ->withSasl(new Sasl(
                config('kafka.username'),
                config('kafka.password'),
                config('kafka.mechanism'),
                config('kafka.security_protocol'),
            ))
            ->withHandler(function(KafkaConsumerMessage $message) {
                $mes = is_string($message->getBody()) ? $message->getBody() : json_encode($message->getBody()) ;
                Log::info('Consumed message: ' . $mes);
                echo "Consumed message: $mes\n";
            })
            ->build();

        return new KafkaQueue($consumer, $producer);
    }
}
