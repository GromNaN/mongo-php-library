<?php
/*
 * Copyright 2015-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

declare(strict_types=1);

namespace MongoDB\Driver;

use MongoDB\BSON\Document;
use MongoDB\Driver\Exception\LogicException;

use function sprintf;

final class BulkWriteCommandResult
{
    private int $insertedCount;
    private int $matchedCount;
    private int $modifiedCount;
    private int $upsertedCount;
    private int $deletedCount;
    private bool $acknowledged;
    private ?Document $insertResults = null;
    private ?Document $updateResults = null;
    private ?Document $deleteResults = null;

    private function __construct()
    {
    }

    public function __debugInfo(): array
    {
        return [
            'isAcknowledged' => $this->acknowledged,
            'insertedCount'  => $this->insertedCount,
            'matchedCount'   => $this->matchedCount,
            'modifiedCount'  => $this->modifiedCount,
            'upsertedCount'  => $this->upsertedCount,
            'deletedCount'   => $this->deletedCount,
            'insertResults'  => $this->insertResults,
            'updateResults'  => $this->updateResults,
            'deleteResults'  => $this->deleteResults,
        ];
    }

    public function getDeletedCount(): int
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->deletedCount;
    }

    /**
     * Returns per-delete results keyed by operation index, or null if verboseResults was not requested.
     */
    public function getDeleteResults(): ?Document
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->deleteResults;
    }

    public function getInsertedCount(): int
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->insertedCount;
    }

    /**
     * Returns per-insert results keyed by operation index, or null if verboseResults was not requested.
     */
    public function getInsertResults(): ?Document
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->insertResults;
    }

    public function getMatchedCount(): int
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->matchedCount;
    }

    public function getModifiedCount(): int
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->modifiedCount;
    }

    /**
     * Returns per-update results keyed by operation index, or null if verboseResults was not requested.
     */
    public function getUpdateResults(): ?Document
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->updateResults;
    }

    public function getUpsertedCount(): int
    {
        $this->assertAcknowledged(__FUNCTION__);

        return $this->upsertedCount;
    }

    public function isAcknowledged(): bool
    {
        return $this->acknowledged;
    }

    /** @internal */
    public static function createFromInternal(
        int $insertedCount,
        int $matchedCount,
        int $modifiedCount,
        int $upsertedCount,
        int $deletedCount,
        bool $acknowledged,
        ?Document $insertResults = null,
        ?Document $updateResults = null,
        ?Document $deleteResults = null,
    ): static {
        $instance = new static();
        $instance->insertedCount = $insertedCount;
        $instance->matchedCount  = $matchedCount;
        $instance->modifiedCount = $modifiedCount;
        $instance->upsertedCount = $upsertedCount;
        $instance->deletedCount  = $deletedCount;
        $instance->acknowledged  = $acknowledged;
        $instance->insertResults = $insertResults;
        $instance->updateResults = $updateResults;
        $instance->deleteResults = $deleteResults;

        return $instance;
    }

    private function assertAcknowledged(string $method): void
    {
        if (! $this->acknowledged) {
            throw new LogicException(
                sprintf(
                    'MongoDB\Driver\BulkWriteCommandResult::%s() should not be called for an unacknowledged write result',
                    $method,
                ),
            );
        }
    }
}
