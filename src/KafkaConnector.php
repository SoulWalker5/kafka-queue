<?php

namespace DiKay\Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        return new KafkaQueue($config);
    }
}
