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

use function implode;
use function is_string;
use function sort;
use function sprintf;
use function str_contains;
use function str_split;

final class Regex implements RegexInterface, JsonSerializable, Type, Stringable
{
    public readonly string $flags;

    public function __construct(public readonly string $pattern, string $flags = '')
    {
        if (str_contains($pattern, "\0")) {
            throw new InvalidArgumentException('Pattern cannot contain null bytes');
        }

        if (str_contains($flags, "\0")) {
            throw new InvalidArgumentException('Flags cannot contain null bytes');
        }

        $this->flags   = self::sortFlags($flags);
    }

    public function __debugInfo(): array
    {
        return [
            'pattern' => $this->pattern,
            'flags'   => $this->flags,
        ];
    }

    public function __serialize(): array
    {
        return [
            'pattern' => $this->pattern,
            'flags'   => $this->flags,
        ];
    }

    public static function __set_state(array $properties): static
    {
        self::validateInitFields($properties);

        return new static($properties['pattern'], $properties['flags']);
    }

    public function __toString(): string
    {
        return sprintf('/%s/%s', $this->pattern, $this->flags);
    }

    public function __unserialize(array $data): void
    {
        self::validateInitFields($data);

        if (str_contains($data['pattern'], "\0")) {
            throw new InvalidArgumentException('Pattern cannot contain null bytes');
        }

        if (str_contains($data['flags'], "\0")) {
            throw new InvalidArgumentException('Flags cannot contain null bytes');
        }

        $this->pattern = $data['pattern'];
        $this->flags   = self::sortFlags($data['flags']);
    }

    public function getFlags(): string
    {
        return $this->flags;
    }

    public function getPattern(): string
    {
        return $this->pattern;
    }

    public function jsonSerialize(): mixed
    {
        return [
            '$regex'   => $this->pattern,
            '$options' => $this->flags,
        ];
    }

    private static function sortFlags(string $flags): string
    {
        if ($flags === '') {
            return '';
        }

        $chars = str_split($flags);
        sort($chars);

        return implode('', $chars);
    }

    private static function validateInitFields(array $data): void
    {
        if (
            ! isset($data['pattern'], $data['flags']) ||
            ! is_string($data['pattern']) ||
            ! is_string($data['flags'])
        ) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Regex initialization requires "pattern" and "flags" string fields',
            );
        }
    }
}
