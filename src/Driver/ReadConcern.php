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

use MongoDB\BSON\Serializable;
use MongoDB\Driver\Exception\InvalidArgumentException;
use stdClass;

use function is_string;

final class ReadConcern implements Serializable
{
    public const LINEARIZABLE = 'linearizable';
    public const LOCAL = 'local';
    public const MAJORITY = 'majority';
    public const AVAILABLE = 'available';
    public const SNAPSHOT = 'snapshot';

    public function __construct(public readonly ?string $level = null)
    {
    }

    public function __serialize(): array
    {
        if ($this->level === null) {
            return [];
        }

        return ['level' => $this->level];
    }

    public static function __set_state(array $properties): static
    {
        $level = $properties['level'] ?? null;

        if ($level !== null && ! is_string($level)) {
            throw InvalidArgumentException::initializationRequiresStringField(self::class, 'level');
        }

        return new static($level);
    }

    public function __unserialize(array $data): void
    {
        $level = $data['level'] ?? null;

        if ($level !== null && ! is_string($level)) {
            throw InvalidArgumentException::initializationRequiresStringField(self::class, 'level');
        }

        $this->level = $level;
    }

    public function bsonSerialize(): stdClass
    {
        $doc = new stdClass();

        if ($this->level !== null) {
            $doc->level = $this->level;
        }

        return $doc;
    }

    public function getLevel(): ?string
    {
        return $this->level;
    }

    public function isDefault(): bool
    {
        return $this->level === null;
    }
}
