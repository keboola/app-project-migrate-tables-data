<?php

declare(strict_types=1);

namespace AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    // @todo implement your custom getters
    public function getFoo(): string
    {
        return $this->getValue(['parameters', 'foo']);
    }
}
