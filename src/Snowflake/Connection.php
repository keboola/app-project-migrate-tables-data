<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Snowflake;

use Keboola\SnowflakeDbAdapter\Connection as AdapterConnection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class Connection extends AdapterConnection
{
    private ?string $region = null;

    private ?string $account = null;

    private string $user;

    private string $database;

    public function __construct(array $options)
    {
        $this->user = $options['user'];
        $this->database = $options['database'];
        parent::__construct($options);
        $this->useRole('ACCOUNTADMIN');
    }

    public function useRole(string $roleName): void
    {
        $this->query(sprintf('USE ROLE %s', QueryBuilder::quoteIdentifier($roleName)));
    }

    public function getCurrentRole(): string
    {
        return $this->fetchAll('SELECT CURRENT_ROLE() AS "role";')[0]['role'];
    }

    public function getRegion(): string
    {
        if (is_null($this->region)) {
            $this->region = $this->fetchAll('SELECT CURRENT_REGION() AS "region";')[0]['region'];
        }
        return $this->region;
    }

    public function getAccount(): string
    {
        if (is_null($this->account)) {
            $this->account = $this->fetchAll('SELECT CURRENT_ACCOUNT() AS "account";')[0]['account'];
        }
        return $this->account;
    }

    public function grantRoleToMigrateUser(string $tableRole): void
    {
        $previousRole = $this->getCurrentRole();
        $this->useRole('ACCOUNTADMIN');
        $this->query(sprintf(
            'GRANT ROLE %s TO USER %s;',
            QueryBuilder::quoteIdentifier($tableRole),
            QueryBuilder::quoteIdentifier($this->user),
        ));
        $this->useRole($previousRole);
    }

    public function grantPrivilegesToReplicaDatabase(string $tableRole): void
    {
        $previousRole = $this->getCurrentRole();
        $this->useRole('ACCOUNTADMIN');
        $this->query(sprintf(
            'GRANT USAGE ON DATABASE %s TO ROLE %s;',
            QueryBuilder::quoteIdentifier($this->database . '_REPLICA'),
            QueryBuilder::quoteIdentifier($tableRole),
        ));

        $this->query(sprintf(
            'GRANT USAGE ON ALL SCHEMAS IN DATABASE %s TO ROLE %s;',
            QueryBuilder::quoteIdentifier($this->database . '_REPLICA'),
            QueryBuilder::quoteIdentifier($tableRole),
        ));

        $this->query(sprintf(
            'GRANT SELECT ON ALL TABLES IN DATABASE %s TO ROLE %s;',
            QueryBuilder::quoteIdentifier($this->database . '_REPLICA'),
            QueryBuilder::quoteIdentifier($tableRole),
        ));

        $this->useRole($previousRole);
    }
}
