<?php

declare(strict_types=1);

namespace AppProjectMigrateLargeTables;

use AppProjectMigrateLargeTables\Exception\SkipTableException;
use AppProjectMigrateLargeTables\FileClient\AbsFileClient;
use AppProjectMigrateLargeTables\FileClient\IFileClient;
use AppProjectMigrateLargeTables\FileClient\S3FileClient;
use Exception;
use GuzzleHttp\Client as GuzzleClient;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\HandlerStack;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

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

    public function run(array $tables): void
    {
        foreach ($tables as $table) {
            $this->migrateTable($table);
        }
    }

    private function migrateTable(string $tableId): void
    {
        try {
            $fileInfo = $this->getTableFileInfo($tableId);
            $tableInfo = $this->sourceClient->getTable($tableId);
        } catch (SkipTableException $e) {
            return;
        }

        $tmp = new Temp();
        $headerFile = $tmp->createFile(sprintf('%s.header.csv', $tableId));
        $headerFile = new CsvFile($headerFile->getPathname());
        $headerFile->writeRow($tableInfo['columns']);

        $tableId = $this->destinationClient->createTableAsync(
            $tableInfo['bucket']['id'],
            $tableInfo['name'],
            $headerFile,
            [
                'primaryKey' => join(',', $tableInfo['primaryKey']),
            ]
        );

        $fileClient = $this->getFileClient($fileInfo);
        if ($fileInfo['isSliced'] === true) {
            $this->logger->info(sprintf('Downloading table %s', $tableId));
            // Download manifest with all sliced files
            $client = new GuzzleClient([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($fileInfo['url'])->getBody()->getContents(), true);
            $options = new FileUploadOptions();
            $options
                ->setFederationToken(true)
                ->setFileName($tableId)
                ->setIsSliced(true)
                ->setIsEncrypted(true)
            ;

            foreach ($manifest['entries'] as $slice) {
                $fileName = $tmp->getTmpFolder() . '/' . basename($slice['url']);
                $downloadedSlices[] = $fileName;

                file_put_contents($fileName, $fileClient->getFileContent($slice));
            }
            $finder = new Finder();
            $slices = $finder->in($tmp->getTmpFolder())->files();
            $listSlices = array_map(fn(SplFileInfo $v) => $v->getPathname(), iterator_to_array($slices));

            $this->logger->info(sprintf('Uploading table %s', $tableId));
            $fileId = $this->destinationClient->uploadSlicedFile($listSlices, $options);

            // Upload data to table
            $this->destinationClient->writeTableAsyncDirect(
                $tableId,
                [
                    'name' => $tableInfo['name'],
                    'dataFileId' => $fileId,
                    'columns' => $tableInfo['columns'],
                ]
            );
        } else {
            $this->logger->warning(sprintf('SKIP table %s', $tableId));
        }
    }

    protected function getTableFileInfo(string $tableId): array
    {
        $table = $this->sourceClient->getTable($tableId);

        if ($table['bucket']['stage'] === 'sys') {
            $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $table['id']));
            throw new SkipTableException();
        }

        if ($table['isAlias']) {
            $this->logger->warning(sprintf('Skipping table %s (alias)', $table['id']));
            throw new SkipTableException();
        }

        $this->logger->info(sprintf('Exporting table %s', $tableId));

        $fileId = $this->sourceClient->exportTableAsync($tableId, [
            'gzip' => true,
        ]);

        return (array) $this->sourceClient->getFile(
            $fileId['file']['id'],
            (new GetFileOptions())->setFederationToken(true)
        );
    }

    protected function getFileClient(array $fileInfo): IFileClient
    {
        if (isset($fileInfo['credentials'])) {
            return new S3FileClient($fileInfo);
        } elseif (isset($fileInfo['absCredentials'])) {
            return new AbsFileClient($fileInfo);
        } else {
            throw new Exception('Unknown file storage client.');
        }
    }
}
