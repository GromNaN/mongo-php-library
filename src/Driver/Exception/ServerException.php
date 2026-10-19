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

use stdClass;
use Throwable;

use function is_array;

class ServerException extends RuntimeException
{
    public object $resultDocument;

    public function __construct(string $message = '', int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->resultDocument = new stdClass();
    }

    final public function getResultDocument(): object
    {
        return $this->resultDocument;
    }

    /**
     * Create an exception from the server reply that caused it.
     *
     * The extension fills the result document from the wire, so it is not a
     * constructor parameter. This factory is the userland equivalent.
     *
     * @internal
     */
    public static function createFromServerReply(string $message, int $code, object $resultDocument, ?Throwable $previous = null): static
    {
        $exception = new static($message, $code, $previous);
        $exception->setResultDocument($resultDocument);

        return $exception;
    }

    /**
     * Attach the server reply that caused this exception.
     *
     * @internal
     */
    public function setResultDocument(object $resultDocument): void
    {
        $this->resultDocument = $resultDocument;

        // Extract errorLabels from the result document (top-level) or writeConcernError.
        $doc = (array) $resultDocument;
        $labels = $doc['errorLabels'] ?? null;
        if ($labels === null) {
            $wce = isset($doc['writeConcernError']) ? (array) $doc['writeConcernError'] : [];
            $labels = $wce['errorLabels'] ?? null;
        }

        if (! is_array($labels)) {
            return;
        }

        $this->errorLabels = $labels;
    }
}
