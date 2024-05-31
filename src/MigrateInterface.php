<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Psr\Log\LoggerInterface;

interface MigrateInterface
{
    public function migrate(Config $config): void;
}
