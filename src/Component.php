<?php

declare(strict_types=1);

namespace AppProjectMigrateLargeTables;

use Keboola\Component\BaseComponent;
use Keboola\StorageApi\Client;

class Component extends BaseComponent
{
    protected function run(): void
    {
        $sourceSapiClient = new Client([
            'url' => $this->getConfig()->getSourceKbcUrl(),
            'token' => $this->getConfig()->getSourceKbcToken(),
        ]);

        $destinationSapiClient = new Client([
            'url' => (string) getenv('KBC_URL'),
            'token' => (string) getenv('KBC_TOKEN'),
        ]);

        $app = new Application($sourceSapiClient, $destinationSapiClient, $this->getLogger());

        $app->run($this->getConfig()->getMigrateTables());
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
