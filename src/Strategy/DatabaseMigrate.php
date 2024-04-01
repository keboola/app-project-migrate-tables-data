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
    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly string $replicaDatabase,
        private readonly Connection $targetConnection,
        private readonly string $targetDatabase,
        private readonly Client $targetSapiClient,
    ) {
    }

    public function migrate(Config $config): void
    {
        $this->refreshReplicaDatabase($config);

        $databaseRole = $this->getSourceRole(
            $this->targetConnection,
            'DATABASE',
            QueryBuilder::quoteIdentifier($this->targetDatabase),
        );
        $this->targetConnection->grantRoleToMigrateUser($databaseRole);
        $this->targetConnection->useRole($databaseRole);

        $this->targetConnection->query(sprintf(
            'USE DATABASE %s;',
            QueryBuilder::quoteIdentifier($this->targetDatabase),
        ));
        $schemas = $this->targetConnection->fetchAll(sprintf(
            'SHOW SCHEMAS IN DATABASE %s;',
            QueryBuilder::quoteIdentifier($this->targetDatabase),
        ));

        foreach ($schemas as $schema) {
            if (in_array($schema['name'], self::SKIP_CLONE_SCHEMAS, true)) {
                continue;
            }
            $this->migrateSchema($config->getMigrateTables(), $schema['name']);
        }
    }

    private function migrateSchema(array $tablesWhiteList, string $schemaName): void
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
            $this->migrateTable($schemaName, $table['name']);
        }
    }

    private function migrateTable(string $schemaName, string $tableName): void
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

        $this->targetConnection->grantPrivilegesToReplicaDatabase(
            $this->replicaDatabase,
            $tableRole,
        );

        $columns = $this->targetConnection->getTableColumns($schemaName, $tableName);

        try {
            $this->targetConnection->query(sprintf(
                'TRUNCATE TABLE %s.%s.%s;',
                QueryBuilder::quoteIdentifier($this->targetDatabase),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));

            $this->targetConnection->query(sprintf(
                'INSERT INTO %s.%s.%s (%s) SELECT %s FROM %s.%s.%s;',
                QueryBuilder::quoteIdentifier($this->targetDatabase),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                QueryBuilder::quoteIdentifier($this->replicaDatabase),
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

    private function refreshReplicaDatabase(Config $config): void
    {
        $currentRole = $this->targetConnection->getCurrentRole();
        $this->targetConnection->useRole('ACCOUNTADMIN');
        $this->targetConnection->query(sprintf(
            'USE DATABASE %s',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
        ));
        $this->targetConnection->query('USE SCHEMA PUBLIC');

        $this->targetConnection->query(sprintf(
            'USE WAREHOUSE %s',
            QueryBuilder::quoteIdentifier($config->getTargetWarehouse()),
        ));

        // Run replicate of data
        $this->logger->info(sprintf('Refreshing replica database %s', $this->replicaDatabase));
        $this->targetConnection->query(sprintf(
            'ALTER DATABASE %s REFRESH',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
        ));
        $this->targetConnection->useRole($currentRole);
    }
}
