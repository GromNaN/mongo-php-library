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

namespace MongoDB\Internal\BSON\Index;

use MongoDB\BSON\Document;
use MongoDB\BSON\PackedArray;
use OutOfBoundsException;
use WeakMap;

use function array_map;
use function sprintf;

/** @internal */
abstract class Index
{
    /** @var WeakMap<Document|PackedArray, self>|null */
    private static ?WeakMap $cache = null;

    /** @var array<string|int, Field> */
    public readonly array $fields;

    final public function __construct(
        Document|PackedArray $structure,
        /** @param list<array{key: string, bsonType: int, keyOffset: int, keyLength: int, dataOffset?: int|null, dataLength?: int|null}> $fields */
        array $fields,
    ) {
        $this->fields = array_map(
            static fn (array $field): Field => new Field(
                $structure,
                $field['key'],
                $field['bsonType'],
                $field['keyOffset'],
                $field['keyLength'],
                $field['dataOffset'] ?? null,
                $field['dataLength'] ?? null,
            ),
            static::sortFields($fields),
        );
    }

    public static function forBson(Document|PackedArray $bson): self
    {
        self::$cache ??= new WeakMap();

        if (! isset(self::$cache[$bson])) {
            $fields             = (new Indexer())->getIndex((string) $bson);
            self::$cache[$bson] = $bson instanceof Document
                ? new DocumentIndex($bson, $fields)
                : new PackedArrayIndex($bson, $fields);
        }

        return self::$cache[$bson];
    }

    public function getField(string|int $key): Field
    {
        return $this->fields[$key] ?? throw new OutOfBoundsException(sprintf('Field "%s" not found in BSON document', $key));
    }

    public function getFieldValue(string|int $key): mixed
    {
        return $this->getField($key)->getValue();
    }

    public function hasField(string|int $key): bool
    {
        return isset($this->fields[$key]);
    }

    abstract protected static function sortFields(array $fields): array;
}
