<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class SapiMigrate implements MigrateInterface
{
    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
    ) {
    }

    public function migrate(Config $config): void
    {
        foreach ($config->getMigrateTables() ?: $this->getAllTables() as $tableId) {
            try {
                $tableInfo = $this->sourceClient->getTable($tableId);
            } catch (ClientException $e) {
                $this->logger->warning(sprintf(
                    'Skipping migration Table ID "%s". Reason: "%s".',
                    $tableId,
                    $e->getMessage(),
                ));
                continue;
            }
            if ($tableInfo['bucket']['stage'] === 'sys') {
                $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $tableInfo['id']));
                continue;
            }

            if ($tableInfo['isAlias']) {
                $this->logger->warning(sprintf('Skipping table %s (alias)', $tableInfo['id']));
                continue;
            }

            $this->migrateTable($tableInfo);
        }
    }

    private function migrateTable(array $sourceTableInfo): void
    {
        $this->logger->info(sprintf('Exporting table %s', $sourceTableInfo['id']));
        $file = $this->sourceClient->exportTableAsync($sourceTableInfo['id'], [
            'gzip' => true,
        ]);

        $sourceFileId = $file['file']['id'];
        $sourceFileInfo = $this->sourceClient->getFile($sourceFileId);

        $tmp = new Temp();
        $optionUploadedFile = new FileUploadOptions();
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($sourceTableInfo['id'])
        ;
        if ($sourceFileInfo['isSliced'] === true) {
            $optionUploadedFile->setIsSliced(true);

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $slices = $this->sourceClient->downloadSlicedFile($sourceFileId, $tmp->getTmpFolder());

            if ($this->dryRun === false) {
                $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
                $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);
            } else {
                $this->logger->info(sprintf('[dry-run] Uploading table %s', $sourceTableInfo['id']));
                $destinationFileId = null;
            }
        } else {
            $fileName = $tmp->getTmpFolder() . '/' . $sourceFileInfo['name'];

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $this->sourceClient->downloadFile($sourceFileId, $fileName);

            if ($this->dryRun === false) {
                $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
                $destinationFileId = $this->targetClient->uploadFile($fileName, $optionUploadedFile);
            } else {
                $this->logger->info(sprintf('[dry-run] Uploading table %s', $sourceTableInfo['id']));
                $destinationFileId = null;
            }
        }

        if ($this->dryRun === false) {
            // Upload data to table
            $this->targetClient->writeTableAsyncDirect(
                $sourceTableInfo['id'],
                [
                    'name' => $sourceTableInfo['name'],
                    'dataFileId' => $destinationFileId,
                    'columns' => $sourceTableInfo['columns'],
                ],
            );
        } else {
            $this->logger->info(sprintf('[dry-run] Import data to table "%s"', $sourceTableInfo['name']));
        }
    }

    private function getAllTables(): array
    {
        $buckets = $this->targetClient->listBuckets();
        $listTables = [];
        foreach ($buckets as $bucket) {
            $bucketTables = $this->targetClient->listTables($bucket['id']);

            // migrate only empty tables
            $filteredBucketTables = array_filter(
                $bucketTables,
                fn($v) => $v['rowsCount'] === 0 || is_null($v['rowsCount']),
            );

            array_unshift(
                $listTables,
                ...array_map(fn($v) => $v['id'], $filteredBucketTables),
            );
        }
        return $listTables;
    }
}
