<?php

declare(strict_types=1);

namespace AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getSourceKbcUrl(): string
    {
        return $this->getValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceKbcToken(): string
    {
        return $this->getValue(['parameters', '#sourceKbcToken']);
    }

    public function getMigrateTables(): array
    {
        return $this->getValue(['parameters', 'tables']);
    }
}
