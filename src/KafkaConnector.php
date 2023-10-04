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
        $producer = Kafka::publishOn($config['queue'], $config['bootstrap_servers'])
            ->withSasl(new Sasl(
                $config['sasl_username'],
                $config['sasl_password'],
                $config['sasl_mechanism'],
                $config['security_protocol'],
            ));

        $consumer = Kafka::createConsumer([$config['bootstrap_servers']])
            ->withSasl(new Sasl(
                $config['sasl_username'],
                $config['sasl_password'],
                $config['sasl_mechanism'],
                $config['security_protocol'],
            ))
            ->withHandler(function(KafkaConsumerMessage $message) {
                $job = unserialize($message->getBody());
                $job->handle();

                $mes = is_string($message->getBody()) ? $message->getBody() : json_encode($message->getBody()) ;
                Log::info('Consumed message: ' . $mes);
                echo "Consumed message: $mes\n";
            })
            ->build();

        return new KafkaQueue($consumer, $producer);
    }
}
