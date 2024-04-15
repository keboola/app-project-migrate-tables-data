<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\AppProjectMigrateLargeTables\Configuration\ConfigDefinition;
use Keboola\AppProjectMigrateLargeTables\Configuration\CreateReplicationsConfigDefinition;
use Keboola\AppProjectMigrateLargeTables\Snowflake\Connection;
use Keboola\AppProjectMigrateLargeTables\Strategy\DatabaseMigrate;
use Keboola\AppProjectMigrateLargeTables\Strategy\DatabaseReplication;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate;
use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\StorageApi\Client;

class Component extends BaseComponent
{
    protected function run(): void
    {
        $sourceSapiClient = new Client([
            'url' => $this->getConfig()->getSourceKbcUrl(),
            'token' => $this->getConfig()->getSourceKbcToken(),
        ]);

        $targetSapiClient = new Client([
            'url' => $this->getConfig()->getEnvKbcUrl(),
            'token' => $this->getConfig()->getEnvKbcToken(),
        ]);
        switch ($this->getConfig()->getMode()) {
            case 'sapi':
                $strategy = new SapiMigrate(
                    $sourceSapiClient,
                    $targetSapiClient,
                    $this->getLogger(),
                );
                break;
            case 'database':
                $verifyToken = $sourceSapiClient->verifyToken();

                $sourceDatabase = sprintf(
                    '%s_%s',
                    $this->getConfig()->getSourceDatabasePrefix(),
                    $verifyToken['owner']['id'],
                );

                $replicaDatabase = sprintf(
                    '%s_%s_REPLICA',
                    $this->getConfig()->getReplicaDatabasePrefix(),
                    $verifyToken['owner']['id'],
                );
                $targetDatabase = sprintf(
                    '%s_%s',
                    $this->getConfig()->getTargetDatabasePrefix(),
                    $targetSapiClient->verifyToken()['owner']['id'],
                );

                $targetConnection = new Connection([
                    'host' => $this->getConfig()->getTargetHost(),
                    'user' => $this->getConfig()->getTargetUser(),
                    'password' => $this->getConfig()->getTargetPassword(),
                    'database' => $targetDatabase,
                ]);

                $strategy = new DatabaseMigrate(
                    $this->getLogger(),
                    $targetConnection,
                    $targetSapiClient,
                    $sourceDatabase,
                    $replicaDatabase,
                    $targetDatabase,
                );
                break;
            default:
                throw new UserException(sprintf('Unknown mode "%s"', $this->getConfig()->getMode()));
        }

        $strategy->migrate($this->getConfig());
    }

    public function createReplicationsAction(): array
    {
        $sourceConnection = new Connection([
            'host' => $this->getConfig()->getSourceHost(),
            'user' => $this->getConfig()->getSourceUser(),
            'password' => $this->getConfig()->getSourcePassword(),
        ]);

        $targetConnection = new Connection([
            'host' => $this->getConfig()->getTargetHost(),
            'user' => $this->getConfig()->getTargetUser(),
            'password' => $this->getConfig()->getTargetPassword(),
        ]);

        $strategy = new DatabaseReplication(
            $this->getLogger(),
            $sourceConnection,
            $targetConnection,
        );
        $strategy->createReplications(
            $this->getConfig(),
            $this->getConfig()->getProjectIdFrom(),
            $this->getConfig()->getProjectIdTo(),
        );

        return ['status' => 'ok'];
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getSyncActions(): array
    {
        return [
            'createReplications' => 'createReplicationsAction',
        ];
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        $rawConfig = $this->getRawConfig();
        $action = $rawConfig['action'] ?? 'run';
        switch ($action) {
            case 'createReplications':
                return CreateReplicationsConfigDefinition::class;
            default:
                return ConfigDefinition::class;
        }
    }
}
