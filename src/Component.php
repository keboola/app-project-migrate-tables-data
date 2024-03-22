<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\AppProjectMigrateLargeTables\Snowflake\Connection;
use Keboola\AppProjectMigrateLargeTables\Strategy\DatabaseMigrate;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate;
use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\StorageApi\Client;

class Component extends BaseComponent
{
    protected function run(): void
    {
        switch ($this->getConfig()->getMode()) {
            case 'sapi':
                $sourceSapiClient = new Client([
                    'url' => $this->getConfig()->getSourceKbcUrl(),
                    'token' => $this->getConfig()->getSourceKbcToken(),
                ]);

                $targetSapiClient = new Client([
                    'url' => (string) getenv('KBC_URL'),
                    'token' => (string) getenv('KBC_TOKEN'),
                ]);
                $strategy = new SapiMigrate(
                    $sourceSapiClient,
                    $targetSapiClient,
                    $this->getLogger(),
                );
                break;
            case 'database':
                $sourceConnection = new Connection([
                    'host' => $this->getConfig()->getSourceHost(),
                    'user' => $this->getConfig()->getSourceUser(),
                    'password' => $this->getConfig()->getSourcePassword(),
                    'database' => $this->getConfig()->getSourceDatabase(),
                ]);
                $targetConnection = new Connection([
                    'host' => $this->getConfig()->getTargetHost(),
                    'user' => $this->getConfig()->getTargetUser(),
                    'password' => $this->getConfig()->getTargetPassword(),
                    'database' => $this->getConfig()->getTargetDatabase(),
                ]);
                $targetSapiClient = new Client([
                    'url' => (string) getenv('KBC_URL'),
                    'token' => (string) getenv('KBC_TOKEN'),
                ]);
                $strategy = new DatabaseMigrate(
                    $sourceConnection,
                    $targetConnection,
                    $targetSapiClient,
                    $this->getLogger(),
                );
                break;
            default:
                throw new UserException(sprintf('Unknown mode "%s"', $this->getConfig()->getMode()));
        }

        $strategy->migrate($this->getConfig());
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
