<?php

declare(strict_types=1);

namespace AppProjectMigrateLargeTables;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class Application
{
    private Client $sourceClient;

    private Client $destinationClient;

    private LoggerInterface $logger;

    public function __construct(Client $sourceClient, Client $destinationClient, LoggerInterface $logger)
    {
        $this->sourceClient = $sourceClient;
        $this->destinationClient = $destinationClient;
        $this->logger = $logger;
    }

    public function run(array $tables = []): void
    {
        foreach ($tables ?: $this->getAllTables() as $tableId) {
            try {
                $tableInfo = $this->sourceClient->getTable($tableId);
            } catch (ClientException $e) {
                $this->logger->warning(sprintf(
                    'Skipping migration Table ID "%s". Reason: "%s".',
                    $tableId,
                    $e->getMessage()
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

            $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
            $destinationFileId = $this->destinationClient->uploadSlicedFile($slices, $optionUploadedFile);
        } else {
            $fileName = $tmp->getTmpFolder() . '/' . $sourceFileInfo['name'];

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $this->sourceClient->downloadFile($sourceFileId, $fileName);

            $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
            $destinationFileId = $this->destinationClient->uploadFile($fileName, $optionUploadedFile);
        }

        // Upload data to table
        $this->destinationClient->writeTableAsyncDirect(
            $sourceTableInfo['id'],
            [
                'name' => $sourceTableInfo['name'],
                'dataFileId' => $destinationFileId,
                'columns' => $sourceTableInfo['columns'],
            ]
        );
    }

    private function getAllTables(): array
    {
        $buckets = $this->destinationClient->listBuckets();
        $listTables = [];
        foreach ($buckets as $bucket) {
            $bucketTables = $this->destinationClient->listTables($bucket['id']);

            // migrate only empty tables
            $filteredBucketTables = array_filter($bucketTables, fn($v) => $v['rowsCount'] === 0);

            $listTables = array_merge(
                array_map(fn($v) => $v['id'], $filteredBucketTables),
                $listTables
            );
        }
        return $listTables;
    }
}
