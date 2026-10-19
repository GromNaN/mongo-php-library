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

namespace MongoDB\Driver\Exception;

use MongoDB\BSON\Document;
use MongoDB\Driver\BulkWriteCommandResult;
use MongoDB\Driver\WriteConcernError;
use MongoDB\Driver\WriteError;

final class BulkWriteCommandException extends ServerException
{
    private ?Document $errorReply = null;

    private ?BulkWriteCommandResult $partialResult = null;

    /** @var list<WriteError> */
    private array $writeErrors = [];

    /** @var list<WriteConcernError> */
    private array $writeConcernErrors = [];

    /**
     * The top-level server error reply document, populated when the server returned an error response.
     */
    public function getErrorReply(): ?Document
    {
        return $this->errorReply;
    }

    /**
     * Partial result containing counts from operations that succeeded before the error occurred.
     */
    public function getPartialResult(): ?BulkWriteCommandResult
    {
        return $this->partialResult;
    }

    /** @return list<WriteConcernError> */
    public function getWriteConcernErrors(): array
    {
        return $this->writeConcernErrors;
    }

    /** @return list<WriteError> */
    public function getWriteErrors(): array
    {
        return $this->writeErrors;
    }

    /** @internal */
    public static function create(
        string $message,
        int $code,
        ?object $resultDocument = null,
        ?Document $errorReply = null,
        ?BulkWriteCommandResult $partialResult = null,
        array $writeErrors = [],
        array $writeConcernErrors = [],
    ): static {
        $instance = new static($message, $code);
        $instance->errorReply        = $errorReply;
        $instance->partialResult     = $partialResult;
        $instance->writeErrors       = $writeErrors;
        $instance->writeConcernErrors = $writeConcernErrors;

        if ($resultDocument !== null) {
            $instance->setResultDocument($resultDocument);
        }

        return $instance;
    }
}
