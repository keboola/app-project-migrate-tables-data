<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\FunctionalTests;

use Keboola\Csv\CsvFile;
use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\DatadirTests\Exception\DatadirTestsException;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\Assert;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    private Client $sourceClient;

    private Client $destinationClient;

    protected function setUp(): void
    {
        parent::setUp();

        $this->sourceClient = new Client([
            'url' => (string) getenv('SOURCE_CLIENT_URL'),
            'token' => (string) getenv('SOURCE_CLIENT_TOKEN'),
        ]);

        $this->destinationClient = new Client([
            'url' => (string) getenv('DESTINATION_CLIENT_URL'),
            'token' => (string) getenv('DESTINATION_CLIENT_TOKEN'),
        ]);

        // remove all buckets/tables from projects
        $this->cleanupProject($this->sourceClient);
        $this->cleanupProject($this->destinationClient);

        $projectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->prepareProject($projectDir . '/source/in/source/', $this->sourceClient);
        $this->prepareProject($projectDir . '/source/in/destination/', $this->destinationClient);
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {
        parent::testDatadir($specification);

        $sourceTables = [];

        foreach ($this->sourceClient->listBuckets() as $bucket) {
            $tables = $this->sourceClient->listTables($bucket['id']);

            $tablesData = array_combine(
                array_map(fn($v) => $v['id'], $tables),
                array_map(fn($v) => $v['rowsCount'], $tables),
            );

            if ($tablesData) {
                $sourceTables = array_merge($tablesData, $sourceTables);
            }
        }

        foreach ($this->destinationClient->listBuckets() as $bucket) {
            foreach ($this->destinationClient->listTables($bucket['id']) as $table) {
                Assert::assertArrayHasKey($table['id'], $sourceTables);
                Assert::assertEquals($sourceTables[$table['id']], $table['rowsCount']);
            }
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanupProject($this->sourceClient);
        $this->cleanupProject($this->destinationClient);
    }

    protected function runScript(string $datadirPath, ?string $runId = null): Process
    {
        $fs = new Filesystem();

        $script = $this->getScript();
        if (!$fs->exists($script)) {
            throw new DatadirTestsException(sprintf(
                'Cannot open script file "%s"',
                $script,
            ));
        }

        $runCommand = [
            'php',
            $script,
        ];
        $runProcess = new Process($runCommand);
        $defaultRunId = random_int(1000, 100000) . '.' . random_int(1000, 100000) . '.' . random_int(1000, 100000);
        $runProcess->setEnv([
            'KBC_DATADIR' => $datadirPath,
            'KBC_URL' => (string) getenv('DESTINATION_CLIENT_URL'),
            'KBC_TOKEN' => (string) getenv('DESTINATION_CLIENT_TOKEN'),
            'KBC_RUNID' => $runId ?? $defaultRunId,
        ]);
        $runProcess->setTimeout(0.0);
        $runProcess->run();
        return $runProcess;
    }

    private function prepareProject(string $dir, Client $client): void
    {
        $finder = new Finder();
        if (!is_dir($dir)) {
            return;
        }
        $files = $finder->in($dir)->files();

        $buckets = array_map(fn($v) => $v['id'], $client->listBuckets());
        foreach ($files as $file) {
            [$bucketStage, $bucketName, $tableName, $suffix] = explode('.', $file->getFilename());
            $bucketId = sprintf('%s.c-%s', $bucketStage, $bucketName);

            if (!in_array($bucketId, $buckets)) {
                $client->createBucket($bucketName, $bucketStage);
                $buckets[] = $bucketId;
            }
            $csv = new CsvFile($file->getPathname());
            $client->createTable($bucketId, $tableName, $csv);
        }
    }

    private function cleanupProject(Client $client): void
    {
        $buckets = $client->listBuckets();
        foreach ($buckets as $bucket) {
            $client->dropBucket($bucket['id'], ['force' => true]);
        }
    }
}
