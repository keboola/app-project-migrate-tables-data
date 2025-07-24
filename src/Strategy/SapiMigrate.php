<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use GuzzleHttp\Utils;
use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;

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
        if ($sourceFileInfo['isSliced'] === true && $sourceFileInfo['provider'] === 'gcp') {
            $this->migrateSlicedGcsFiles(
                $sourceFileId,
                $sourceTableInfo,
                $config->preserveTimestamp(),
                $tmp,
            );
            $tmp->remove();
            return;
        } elseif ($sourceFileInfo['isSliced'] === true) {
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

    private function migrateSlicedGcsFiles(
        int $fileId,
        array $tableInfo,
        bool $preserveTimestamp,
        Temp $tmpFolder,
    ): void {
        $fileInfo = $this->sourceClient->getFile(
            $fileId,
            (new GetFileOptions())->setFederationToken(true),
        );

        $bucket = $fileInfo['gcsPath']['bucket'];
        $gcsClient = $this->getGcsClient($fileId);
        $retBucket = $gcsClient->bucket($bucket);
        $manifestObject = $retBucket->object($fileInfo['gcsPath']['key'] . 'manifest')->downloadAsString();

        /** @var array{"entries": string[]} $manifest */
        $manifest = Utils::jsonDecode($manifestObject, true);
        $chunks = array_chunk((array) $manifest['entries'], 50);

        $optionUploadedFile = new FileUploadOptions();
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($tableInfo['id'])
            ->setIsSliced(true)
        ;

        foreach ($chunks as $chunkKey => $chunk) {
            $this->logger->info(sprintf('Migrating %s chunk', $chunkKey));
            $slices = [];
            // refresh credentials for each chunk
            $gcsClient = $this->getGcsClient($fileId);
            $retBucket = $gcsClient->bucket($bucket);
            /** @var array{"url": string} $entry */
            foreach ($chunk as $entry) {
                $this->logger->info(sprintf('Migrating %s file.', $entry['url']));
                $slices[] = $destinationFile = $tmpFolder->getTmpFolder() . '/' . basename($entry['url']);

                $sprintf = sprintf(
                    '/%s/',
                    $fileInfo['gcsPath']['bucket'],
                );
                $blobPath = explode($sprintf, $entry['url']);
                $retBucket->object($blobPath[1])->downloadToFile($destinationFile);
            }

            $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);

            $this->targetClient->writeTableAsyncDirect(
                $tableInfo['id'],
                [
                    'name' => $tableInfo['name'],
                    'dataFileId' => $destinationFileId,
                    'columns' => $tableInfo['columns'],
                    'useTimestampFromDataFile' => $preserveTimestamp,
                ],
            );

            $fs = new Filesystem();
            foreach ($slices as $slice) {
                $fs->remove($slice);
            }
        }
    }

    private function getGcsClient(int $fileId): GoogleStorageClient
    {
        $fileInfo = $this->sourceClient->getFile(
            $fileId,
            (new GetFileOptions())->setFederationToken(true),
        );
        $gcsCredentials = $fileInfo['gcsCredentials'];
        $options = [
            'credentials' => [
                'access_token' => $gcsCredentials['access_token'],
                'expires_in' => $gcsCredentials['expires_in'],
                'token_type' => $gcsCredentials['token_type'],
            ],
            'projectId' => $gcsCredentials['projectId'],
        ];

        $fetchAuthToken = $this->getAuthTokenClass($options['credentials']);
        return new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);
    }

    private function getAuthTokenClass(array $credentials): FetchAuthTokenInterface
    {
        return new class ($credentials) implements FetchAuthTokenInterface {

            public function __construct(
                private array $creds,
            ) {
            }

            public function fetchAuthToken(?callable $httpHandler = null)
            {
                return $this->creds;
            }

            public function getCacheKey()
            {
                return '';
            }

            public function getLastReceivedToken()
            {
                return $this->creds;
            }
        };
    }
}
