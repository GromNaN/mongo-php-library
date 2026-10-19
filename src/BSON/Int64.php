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

use JsonSerializable;
use MongoDB\Driver\Exception\InvalidArgumentException;
use Stringable;

use function is_int;
use function is_string;
use function ltrim;
use function preg_match;
use function sprintf;
use function str_starts_with;
use function strlen;

final class Int64 implements JsonSerializable, Type, Stringable
{
    public readonly string $integer;

    public function __construct(int|string $value)
    {
        if (is_int($value)) {
            $this->integer = (string) $value;

            return;
        }

        $this->integer = self::parseString($value);
    }

    public function __debugInfo(): array
    {
        return ['integer' => $this->integer];
    }

    public function __serialize(): array
    {
        return ['integer' => $this->integer];
    }

    public static function __set_state(array $properties): static
    {
        if (! is_string($properties['integer'] ?? null)) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Int64 initialization requires "integer" string field',
            );
        }

        return new static($properties['integer']);
    }

    public function __toString(): string
    {
        return $this->integer;
    }

    public function __unserialize(array $data): void
    {
        if (! is_string($data['integer'] ?? null)) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Int64 initialization requires "integer" string field',
            );
        }

        $this->integer = self::parseString($data['integer']);
    }

    public function jsonSerialize(): mixed
    {
        return ['$numberLong' => $this->integer];
    }

    private static function parseString(string $value): string
    {
        if (! preg_match('/^-?\d+$/', $value)) {
            throw new InvalidArgumentException(
                sprintf('Error parsing "%s" as 64-bit integer for MongoDB\BSON\Int64 initialization', $value),
            );
        }

        // Strip leading zeros (but keep a single "0") to normalise before range check.
        $normalized = ltrim($value, '-0') ?: '0';
        $isNegative = str_starts_with($value, '-');

        // INT64_MIN = -9223372036854775808, INT64_MAX = 9223372036854775807
        $limit = $isNegative ? '9223372036854775808' : '9223372036854775807';

        if (strlen($normalized) > strlen($limit) || (strlen($normalized) === strlen($limit) && $normalized > $limit)) {
            throw new InvalidArgumentException(
                sprintf('Error parsing "%s" as 64-bit integer for MongoDB\BSON\Int64 initialization', $value),
            );
        }

        return $value;
    }
}
