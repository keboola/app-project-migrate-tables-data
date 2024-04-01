<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    private const DATABASE_PREFIXES = [
        'connection.keboola.com' => [
            'db_replica_prefix' => 'AWSUS',
            'db_prefix' => 'SAPI',
        ],
        'connection.eu-central-1.keboola.com' => [
            'db_replica_prefix' => 'AWSEU',
            'db_prefix' => 'KEBOOLA',
        ],
        'connection.north-europe.azure.keboola.com' => [
            'db_replica_prefix' => 'AZNE',
            'db_prefix' => 'KEBOOLA',
        ],
        'connection.europe-west3.gcp.keboola.com' => [
            'db_replica_prefix' => 'GCPEUW3',
            'db_prefix' => 'KBC_EUW3',
        ],
        'connection.us-east4.gcp.keboola.com' => [
            'db_replica_prefix' => 'GCPUSE4',
            'db_prefix' => 'KBC_USE4',
        ],
    ];

    public function getMode(): string
    {
        return $this->getStringValue(['parameters', 'mode']);
    }

    public function getSourceKbcUrl(): string
    {
        return $this->getStringValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceKbcToken(): string
    {
        return $this->getStringValue(['parameters', '#sourceKbcToken']);
    }

    public function getMigrateTables(): array
    {
        return $this->getArrayValue(['parameters', 'tables']);
    }

    public function getTargetHost(): string
    {
        return $this->getImageParameters()['db']['host'];
    }

    public function getTargetUser(): string
    {
        return $this->getImageParameters()['db']['username'];
    }

    public function getTargetPassword(): string
    {
        return $this->getImageParameters()['db']['#password'];
    }

    public function getTargetWarehouse(): string
    {
        return $this->getImageParameters()['db']['warehouse'];
    }

    public function getSourceDatabasePrefix(): string
    {
        $url = parse_url($this->getSourceKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::DATABASE_PREFIXES[$url['host']]['db_prefix'];
    }

    public function getReplicaDatabasePrefix(): string
    {
        $url = parse_url($this->getSourceKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::DATABASE_PREFIXES[$url['host']]['db_replica_prefix'];
    }

    public function getTargetDatabasePrefix(): string
    {
        $url = parse_url($this->getEnvKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::DATABASE_PREFIXES[$url['host']]['db_prefix'];
    }

    public function getSourceHost(): string
    {
        return $this->getStringValue(['parameters', 'sourceHost']);
    }

    public function getSourceUser(): string
    {
        return $this->getStringValue(['parameters', 'sourceUsername']);
    }

    public function getSourcePassword(): string
    {
        return $this->getStringValue(['parameters', '#sourcePassword']);
    }

    public function getProjectIdFrom(): int
    {
        return $this->getIntValue(['parameters', 'projectIdFrom']);
    }

    public function getProjectIdTo(): int
    {
        return $this->getIntValue(['parameters', 'projectIdTo']);
    }
}
