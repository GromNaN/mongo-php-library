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

namespace MongoDB\BSON;

use ArrayIterator;
use Exception;
use MongoDB\Driver\Exception\LogicException;
use MongoDB\Internal\BSON\Index\Field;

/**
 * A forward-only iterator over a decoded BSON document or array.
 *
 * Instances are created exclusively by internal decoder code via
 * {@see self::createFromDecodedData()}.  The constructor is private to
 * prevent userland instantiation.
 *
 * @method void next()
 * @method void rewind()
 * @method bool valid()
 */
final class Iterator extends ArrayIterator
{
    /**
     * Private constructor – use {@see self::createFromDecodedData()} instead.
     *
     * @param array<string|int, Field> $fields
     */
    private function __construct(public readonly Document|PackedArray $bson, array $fields)
    {
    }

    public function __debugInfo(): array
    {
        return ['bson' => $this->bson];
    }

    public function __serialize(): array
    {
        throw new Exception("Serialization of 'MongoDB\\BSON\\Iterator' is not allowed");
    }

    public function current(): mixed
    {
        if (! $this->valid()) {
            throw new LogicException('Cannot call current() on an exhausted iterator');
        }

        return parent::current()->getValue();
    }

    public function key(): string|int
    {
        if (! $this->valid()) {
            throw new LogicException('Cannot call key() on an exhausted iterator');
        }

        return parent::key();
    }

    /**
     * Factory method intended for use by {@see Document} and {@see PackedArray}.
     *
     * @internal
     *
     * @param array<string|int, Field> $fields
     */
    public static function createFromDecodedData(Document|PackedArray $bson, array $fields): static
    {
        return new static($bson, $fields);
    }
}
