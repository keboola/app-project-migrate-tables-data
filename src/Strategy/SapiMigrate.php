<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class SapiMigrate implements MigrateInterface
{
    private StorageModifier $storageModifier;

    /** @var string[] $bucketsExist */
    private array $bucketsExist = [];

    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
    ) {
        $this->storageModifier = new StorageModifier($this->targetClient);
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

            if (!in_array($tableInfo['bucket']['id'], $this->bucketsExist) &&
                !$this->targetClient->bucketExists($tableInfo['bucket']['id'])) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating bucket %s', $tableInfo['bucket']['id']));
                } else {
                    $this->logger->info(sprintf('Creating bucket %s', $tableInfo['bucket']['id']));
                    $this->bucketsExist[] = $tableInfo['bucket']['id'];

                    $this->storageModifier->createBucket($tableInfo['bucket']['id']);
                }
            }

            if (!$this->targetClient->tableExists($tableId)) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating table %s', $tableInfo['id']));
                } else {
                    $this->logger->info(sprintf('Creating table %s', $tableInfo['id']));
                    $this->storageModifier->createTable($tableInfo);
                }
            }

            $this->migrateTable($tableInfo, $config);
        }
    }

    private function migrateTable(array $sourceTableInfo, Config $config): void
    {
        $this->logger->info(sprintf('Exporting table %s', $sourceTableInfo['id']));
        $file = $this->sourceClient->exportTableAsync($sourceTableInfo['id'], [
            'gzip' => true,
            'includeInternalTimestamp' => $config->preserveTimestamp(),
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
                    'useTimestampFromDataFile' => $config->preserveTimestamp(),
                ],
            );
        } else {
            $this->logger->info(sprintf('[dry-run] Import data to table "%s"', $sourceTableInfo['name']));
        }

        $tmp->remove();
    }

    private function getAllTables(): array
    {
        $buckets = $this->sourceClient->listBuckets();
        $listTables = [];
        foreach ($buckets as $bucket) {
            $sourceBucketTables = $this->sourceClient->listTables($bucket['id']);
            if (!$this->targetClient->bucketExists($bucket['id'])) {
                $targetBucketTables = [];
            } else {
                $targetBucketTables = $this->targetClient->listTables($bucket['id']);
            }

            $filteredBucketTables = array_filter(
                $sourceBucketTables,
                function ($sourceTable) use ($targetBucketTables) {
                    $v = current(array_filter(
                        $targetBucketTables,
                        fn($v) => $v['id'] === $sourceTable['id'],
                    ));
                    return empty($v) || $v['rowsCount'] === 0 || is_null($v['rowsCount']);
                },
            );

            array_unshift(
                $listTables,
                ...array_map(fn($v) => $v['id'], $filteredBucketTables),
            );
        }
        return $listTables;
    }
}
