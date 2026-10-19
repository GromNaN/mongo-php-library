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

use MongoDB\Driver\WriteResult;
use Throwable;

class BulkWriteException extends ServerException
{
    /**
     * @psalm-suppress PropertyNotSetInConstructor The factory sets it before
     * the exception is thrown. The extension fills it from the wire.
     */
    public WriteResult $writeResult;

    final public function getWriteResult(): WriteResult
    {
        return $this->writeResult;
    }

    /**
     * Create an exception from the write result that caused it.
     *
     * The extension fills the write result from the wire, so it is not a
     * constructor parameter. This factory is the userland equivalent.
     *
     * @internal
     */
    public static function create(string $message, int $code, WriteResult $writeResult, ?object $resultDocument = null, ?Throwable $previous = null): static
    {
        $exception = new static($message, $code, $previous);
        $exception->writeResult = $writeResult;

        if ($resultDocument !== null) {
            $exception->setResultDocument($resultDocument);
        }

        return $exception;
    }
}
