<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
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

    public function getSourceHost(): string
    {
        $stack = parse_url($this->getSourceKbcUrl());
        assert($stack && array_key_exists('host', $stack));
        return $this->getImageParameters()['db']['source'][$stack['host']]['host'];
    }

    public function getSourceUser(): string
    {
        $stack = parse_url($this->getSourceKbcUrl());
        assert($stack && array_key_exists('host', $stack));
        return $this->getImageParameters()['db']['source'][$stack['host']]['username'];
    }

    public function getSourcePassword(): string
    {
        $stack = parse_url($this->getSourceKbcUrl());
        assert($stack && array_key_exists('host', $stack));
        return $this->getImageParameters()['db']['source'][$stack['host']]['#password'];
    }

    public function getSourceDatabase(): string
    {
        return $this->getStringValue(['parameters', 'sourceDatabase']);
    }

    public function getTargetHost(): string
    {
        return $this->getImageParameters()['db']['target']['host'];
    }

    public function getTargetUser(): string
    {
        return $this->getImageParameters()['db']['target']['username'];
    }

    public function getTargetPassword(): string
    {
        return $this->getImageParameters()['db']['target']['#password'];
    }

    public function getTargetDatabase(): string
    {
        return $this->getStringValue(['parameters', 'targetDatabase']);
    }

    public function getTargetWarehouse(): string
    {
        return $this->getImageParameters()['db']['target']['warehouse'];
    }
}
