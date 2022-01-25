<?php

declare(strict_types=1);

namespace AppProjectMigrateLargeTables\FileClient;

interface IFileClient
{
    /** @return resource */
    public function getFileContent(?array $filePart = null);
}
