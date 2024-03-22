<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\Snowflake\Connection;
use Keboola\Csv\CsvFile;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;
use Throwable;

class DatabaseMigrate implements MigrateInterface
{

    public function __construct(
        private readonly Connection $sourceConnection,
        private readonly Connection $targetConnection,
        private readonly Client $targetSapiClient,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function migrate(Config $config): void
    {
        $this->createReplication(
            $config->getSourceDatabase(),
            $config->getTargetDatabase(),
            $config->getTargetWarehouse(),
        );

        $databaseRole = $this->getSourceRole(
            $this->targetConnection,
            'DATABASE',
            QueryBuilder::quoteIdentifier($config->getTargetDatabase()),
        );
        $this->targetConnection->grantRoleToMigrateUser($databaseRole);
        $this->targetConnection->useRole($databaseRole);

        $this->targetConnection->query(sprintf(
            'USE DATABASE %s;',
            QueryBuilder::quoteIdentifier($config->getTargetDatabase()),
        ));
        $schemas = $this->targetConnection->fetchAll(sprintf(
            'SHOW SCHEMAS IN DATABASE %s;',
            QueryBuilder::quoteIdentifier($config->getTargetDatabase()),
        ));

        foreach ($schemas as $schema) {
            $this->migrateSchema($config->getMigrateTables(), $config->getTargetDatabase(), $schema['name']);
        }
    }

    private function createReplication(string $sourceDatabase, string $targetDatabase, string $targetWarehouse): void
    {
        $replicaDatabase = $targetDatabase . '_REPLICA';

        // Allow replication on source database
        $this->logger->info(sprintf('Enabling replication on database %s', $sourceDatabase));
        $this->sourceConnection->query(sprintf(
            'ALTER DATABASE %s ENABLE REPLICATION TO ACCOUNTS %s.%s;',
            QueryBuilder::quoteIdentifier($sourceDatabase),
            $this->targetConnection->getRegion(),
            $this->targetConnection->getAccount(),
        ));

        // Waiting for previous SQL query
        sleep(5);

        // Migration database sqls
        $this->logger->info(sprintf('Creating replica database %s', $replicaDatabase));
        $this->targetConnection->query(sprintf(
            'CREATE DATABASE IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
            QueryBuilder::quoteIdentifier($replicaDatabase),
            $this->sourceConnection->getRegion(),
            $this->sourceConnection->getAccount(),
            QueryBuilder::quoteIdentifier($sourceDatabase),
        ));

        $this->targetConnection->query(sprintf(
            'USE DATABASE %s',
            QueryBuilder::quoteIdentifier($replicaDatabase),
        ));
        $this->targetConnection->query('USE SCHEMA PUBLIC');

        $this->targetConnection->query(sprintf(
            'USE WAREHOUSE %s',
            QueryBuilder::quoteIdentifier($targetWarehouse),
        ));

        // Run replicate of data
        $this->logger->info(sprintf('Refreshing replica database %s', $replicaDatabase));
        $this->targetConnection->query(sprintf(
            'ALTER DATABASE %s REFRESH',
            QueryBuilder::quoteIdentifier($replicaDatabase),
        ));

        $this->logger->info(sprintf('Replica database %s created', $replicaDatabase));
    }

    private function migrateSchema(array $tablesWhiteList, string $database, string $schemaName): void
    {
        $this->logger->info(sprintf('Migrating schema %s', $schemaName));
        $tables = $this->targetConnection->fetchAll(sprintf(
            'SHOW TABLES IN SCHEMA %s;',
            QueryBuilder::quoteIdentifier($schemaName),
        ));

        foreach ($tables as $table) {
            $tableId = sprintf('%s.%s', $schemaName, $table['name']);
            if ($tablesWhiteList && !in_array($tableId, $tablesWhiteList, true)) {
                continue;
            }
            $this->migrateTable($database, $schemaName, $table['name']);
        }
    }

    private function migrateTable(string $database, string $schemaName, string $tableName): void
    {
        $this->logger->info(sprintf('Migrating table %s.%s', $schemaName, $tableName));
        $tableRole = $this->getSourceRole(
            $this->targetConnection,
            'TABLE',
            QueryBuilder::quoteIdentifier($schemaName) . '.' . QueryBuilder::quoteIdentifier($tableName),
        );

        try {
            $this->targetConnection->useRole($tableRole);
        } catch (Throwable) {
            $this->targetConnection->grantRoleToMigrateUser($tableRole);
            $this->targetConnection->useRole($tableRole);
        }

        $this->targetConnection->grantPrivilegesToReplicaDatabase($tableRole);

        $columns = $this->targetConnection->getTableColumns($schemaName, $tableName);

        try {
            $this->targetConnection->query(sprintf(
                'TRUNCATE TABLE %s.%s.%s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));

            $this->targetConnection->query(sprintf(
                'INSERT INTO %s.%s.%s (%s) SELECT %s FROM %s.%s.%s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                QueryBuilder::quoteIdentifier($database . '_REPLICA'),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));
        } catch (RuntimeException $e) {
            $this->logger->warning(sprintf(
                'Error while migrating table %s.%s: %s',
                $schemaName,
                $tableName,
                $e->getMessage(),
            ));
            return;
        }

        $this->logger->info(sprintf('Refreshing table %s.%s metadata', $schemaName, $tableName));
        $columns = array_filter($columns, fn($v) => $v !== '_timestamp');
        $csv = $this->createDataFile($columns);
        $tableId = sprintf('%s.%s', $schemaName, $tableName);
        if (!$this->targetSapiClient->tableExists($tableId)) {
            $this->logger->warning(sprintf('Table %s does not exist in Storage API', $tableId));
        }
        $this->targetSapiClient->writeTableAsync($tableId, $csv, ['incremental' => true]);
    }

    private function getSourceRole(Connection $connection, string $showGrantsOn, string $targetSourceName): string
    {
        $grantsOnDatabase = $connection->fetchAll(sprintf(
            'SHOW GRANTS ON %s %s;',
            $showGrantsOn,
            $targetSourceName,
        ));

        $ownershipOnDatabase = array_filter($grantsOnDatabase, fn($v) => $v['privilege'] === 'OWNERSHIP');
        assert(count($ownershipOnDatabase) === 1);

        return current($ownershipOnDatabase)['grantee_name'];
    }

    private function createDataFile(array $columns): CsvFile
    {
        $csv = new CsvFile('/tmp/tempDataFile.csv');
        $csv->writeRow($columns);
        return $csv;
    }
}
