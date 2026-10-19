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

use function sprintf;

final class WriteResult
{
    public readonly ?WriteConcern $writeConcern;

    /**
     * Private constructor. Use createFromInternal() to create instances.
     *
     * @param array<int, mixed> $upsertedIds
     * @param WriteError[]      $writeErrors
     * @param object[]          $errorReplies
     */
    private function __construct(
        public readonly ?int $insertedCount,
        public readonly ?int $matchedCount,
        public readonly ?int $modifiedCount,
        public readonly ?int $deletedCount,
        public readonly ?int $upsertedCount,
        public readonly array $upsertedIds,
        public readonly Server $server,
        public readonly ?WriteConcernError $writeConcernError,
        public readonly array $writeErrors,
        private bool $acknowledged,
        ?WriteConcern $writeConcern = null,
        public readonly array $errorReplies = [],
    ) {
        $this->writeConcern = $writeConcern ?? WriteConcern::createDefault();
    }

    public function __debugInfo(): array
    {
        $upsertedIdsList = [];
        foreach ($this->upsertedIds as $index => $id) {
            $upsertedIdsList[] = ['index' => $index, '_id' => $id];
        }

        return [
            'nInserted'         => $this->insertedCount,
            'nMatched'          => $this->matchedCount,
            'nModified'         => $this->modifiedCount,
            'nRemoved'          => $this->deletedCount,
            'nUpserted'         => $this->upsertedCount,
            'upsertedIds'       => $upsertedIdsList,
            'writeErrors'       => $this->writeErrors,
            'writeConcernError' => $this->writeConcernError,
            'writeConcern'      => $this->writeConcern,
            'errorReplies'      => $this->errorReplies,
        ];
    }

    public function getDeletedCount(): int
    {
        $this->assertAcknowledged(__METHOD__);

        return $this->deletedCount;
    }

    public function getErrorReplies(): array
    {
        return $this->errorReplies;
    }

    public function getInsertedCount(): int
    {
        $this->assertAcknowledged(__METHOD__);

        return $this->insertedCount;
    }

    public function getMatchedCount(): int
    {
        $this->assertAcknowledged(__METHOD__);

        return $this->matchedCount;
    }

    public function getModifiedCount(): int
    {
        $this->assertAcknowledged(__METHOD__);

        return $this->modifiedCount;
    }

    public function getServer(): Server
    {
        return $this->server;
    }

    public function getUpsertedCount(): int
    {
        $this->assertAcknowledged(__METHOD__);

        return $this->upsertedCount;
    }

    public function getUpsertedIds(): array
    {
        $this->assertAcknowledged(__METHOD__);

        return $this->upsertedIds;
    }

    public function getWriteConcernError(): ?WriteConcernError
    {
        return $this->writeConcernError;
    }

    /** @return WriteError[] */
    public function getWriteErrors(): array
    {
        return $this->writeErrors;
    }

    public function isAcknowledged(): bool
    {
        return $this->acknowledged;
    }

    /**
     * @internal Creates a new WriteResult instance.
     * Used by \MongoDB\Driver\Server.
     *
     * @param array<int, mixed> $upsertedIds
     * @param WriteError[]      $writeErrors
     * @param object[]          $errorReplies
     */
    public static function createFromInternal(
        ?int $insertedCount,
        ?int $matchedCount,
        ?int $modifiedCount,
        ?int $deletedCount,
        ?int $upsertedCount,
        array $upsertedIds,
        Server $server,
        ?WriteConcernError $writeConcernError,
        array $writeErrors,
        bool $acknowledged,
        ?WriteConcern $writeConcern = null,
        array $errorReplies = [],
    ): static {
        return new static(
            $insertedCount,
            $matchedCount,
            $modifiedCount,
            $deletedCount,
            $upsertedCount,
            $upsertedIds,
            $server,
            $writeConcernError,
            $writeErrors,
            $acknowledged,
            $writeConcern,
            $errorReplies,
        );
    }

    private function assertAcknowledged(string $method): void
    {
        if ($this->acknowledged) {
            return;
        }

        throw new Exception\LogicException(
            sprintf('%s() should not be called for an unacknowledged write result', $method),
        );
    }
}
