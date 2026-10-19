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

use InvalidArgumentException;
use MongoDB\Internal\BSON\BsonDecoder;
use MongoDB\Internal\BSON\BsonType;
use OutOfBoundsException;
use Stringable;
use WeakReference;

/** @internal */
final class Field
{
    /** @var WeakReference<Stringable> */
    private readonly WeakReference $source;
    private bool $isInitialized = false;
    private mixed $value;

    public function __construct(
        Stringable $source,
        public readonly string $key,
        public readonly int $bsonType,
        public readonly int $keyOffset,
        public readonly int $keyLength,
        public readonly int|null $dataOffset = null,
        public readonly int|null $dataLength = null,
    ) {
        if (
            $this->bsonType === BsonType::UNDEFINED
            || $this->bsonType === BsonType::NULL
            || $this->bsonType === BsonType::MINKEY
            || $this->bsonType === BsonType::MAXKEY
        ) {
            if ($this->dataLength !== 0 || $this->dataOffset !== null) {
                throw new InvalidArgumentException('Invalid data offset or length');
            }
        } else {
            if ($this->dataLength === null || $this->dataOffset === null) {
                throw new InvalidArgumentException('Invalid data offset or length');
            }

            if ($this->dataLength < 0 || $this->dataOffset <= $this->keyOffset + $this->keyLength) {
                throw new InvalidArgumentException('Invalid data offset or length');
            }
        }

        $this->source = WeakReference::create($source);
    }

    public function getValue(): mixed
    {
        if (! $this->isInitialized) {
            $this->readValue();
        }

        return $this->value;
    }

    private function readValue(): void
    {
        $source = $this->source->get();
        if (! $source instanceof Stringable) {
            throw new OutOfBoundsException('BSON document is no longer valid');
        }

        $this->value         = BsonDecoder::decodeFieldValue((string) $source, $this->bsonType, (int) $this->dataOffset);
        $this->isInitialized = true;
    }
}
