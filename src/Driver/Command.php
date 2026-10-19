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
use MongoDB\Driver\Exception\UnexpectedValueException;
use stdClass;

use function is_array;

final class Command
{
    private array $options;

    public function __construct(private array|object $document, ?array $commandOptions = null)
    {
        if (! is_document($document)) {
            throw UnexpectedValueException::documentRequiredAsRoot();
        }

        $this->options = $commandOptions ?? [];
    }

    public function __debugInfo(): array
    {
        $doc = $this->document;
        if ($doc instanceof Document) {
            $doc = $doc->toPHP();
        } elseif (is_array($doc)) {
            $obj = new stdClass();
            foreach ($doc as $k => $v) {
                $obj->$k = $v;
            }

            $doc = $obj;
        }

        return ['command' => $doc];
    }

    /** @internal */
    public function getDocument(): array|object
    {
        return $this->document;
    }

    /** @internal */
    public function getOptions(): array
    {
        return $this->options;
    }
}
