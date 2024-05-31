<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\Snowflake\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Psr\Log\LoggerInterface;

class DatabaseReplication
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly Connection $sourceConnection,
        private readonly Connection $targetConnection,
    ) {
    }

    public function createReplications(
        Config $config,
        int $fromProjectId,
        int $toProjectId,
    ): void {
        $databases = array_map(fn($v) => $v['name'], $this->sourceConnection->fetchAll('SHOW DATABASES;'));
        for ($i = $fromProjectId; $i <= $toProjectId; $i++) {
            $sourceDatabase = sprintf('%s_%s', $config->getSourceDatabasePrefix(), $i);
            if (!in_array($sourceDatabase, $databases, true)) {
                continue;
            }
            $this->createReplication($sourceDatabase);
        }
    }

    public function createReplication(string $sourceDatabase): void
    {
        // Allow replication on source database
        $this->logger->info(sprintf('Enabling replication on database %s', $sourceDatabase));

        $this->sourceConnection->query(sprintf(
            'ALTER DATABASE %s ENABLE REPLICATION TO ACCOUNTS %s.%s;',
            QueryBuilder::quoteIdentifier($sourceDatabase),
            $this->targetConnection->getRegion(),
            $this->targetConnection->getAccount(),
        ));
    }
}
