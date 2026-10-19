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
use function preg_match;
use function sprintf;

final class Timestamp implements TimestampInterface, JsonSerializable, Type, Stringable
{
    public readonly string $increment;
    public readonly string $timestamp;

    public function __construct(int|string $increment, int|string $timestamp)
    {
        $this->increment = is_string($increment)
            ? self::parseStringField($increment, 'increment')
            : self::validateIntField($increment, 'increment');

        $this->timestamp = is_string($timestamp)
            ? self::parseStringField($timestamp, 'timestamp')
            : self::validateIntField($timestamp, 'timestamp');
    }

    public function __debugInfo(): array
    {
        return [
            'increment' => $this->increment,
            'timestamp' => $this->timestamp,
        ];
    }

    public function __serialize(): array
    {
        return [
            'increment' => $this->increment,
            'timestamp' => $this->timestamp,
        ];
    }

    public static function __set_state(array $properties): static
    {
        self::validateInitFields($properties);

        return new static($properties['increment'], $properties['timestamp']);
    }

    public function __toString(): string
    {
        return sprintf('[%s:%s]', $this->increment, $this->timestamp);
    }

    public function __unserialize(array $data): void
    {
        self::validateInitFields($data);

        if (is_string($data['increment'])) {
            $this->increment = self::parseStringField($data['increment'], 'increment');
            $this->timestamp = self::parseStringField($data['timestamp'], 'timestamp');
        } else {
            $this->increment = self::validateIntField($data['increment'], 'increment');
            $this->timestamp = self::validateIntField($data['timestamp'], 'timestamp');
        }
    }

    public function getIncrement(): int
    {
        return (int) $this->increment;
    }

    public function getTimestamp(): int
    {
        return (int) $this->timestamp;
    }

    public function jsonSerialize(): mixed
    {
        return [
            '$timestamp' => [
                't' => (int) $this->timestamp,
                'i' => (int) $this->increment,
            ],
        ];
    }

    private static function parseStringField(string $value, string $field): string
    {
        if (! preg_match('/^-?\d+$/', $value)) {
            throw new InvalidArgumentException(
                sprintf('Error parsing "%s" as 64-bit integer %s for MongoDB\BSON\Timestamp initialization', $value, $field),
            );
        }

        $int = (int) $value;

        if ((string) $int !== $value) {
            throw new InvalidArgumentException(
                sprintf('Error parsing "%s" as 64-bit integer %s for MongoDB\BSON\Timestamp initialization', $value, $field),
            );
        }

        if ($int < 0 || $int > 0xFFFFFFFF) {
            throw new InvalidArgumentException(
                sprintf('Expected %s to be an unsigned 32-bit integer, %d given', $field, $int),
            );
        }

        return $value;
    }

    private static function validateInitFields(array $data): void
    {
        if (
            ! isset($data['increment'], $data['timestamp']) ||
            ! (
                (is_int($data['increment']) && is_int($data['timestamp'])) ||
                (is_string($data['increment']) && is_string($data['timestamp']))
            )
        ) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Timestamp initialization requires "increment" and "timestamp" integer or numeric string fields',
            );
        }
    }

    private static function validateIntField(int $value, string $field): string
    {
        if ($value < 0 || $value > 0xFFFFFFFF) {
            throw new InvalidArgumentException(
                sprintf('Expected %s to be an unsigned 32-bit integer, %d given', $field, $value),
            );
        }

        return (string) $value;
    }
}
