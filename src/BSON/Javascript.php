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
use MongoDB\Driver\Exception\UnexpectedValueException;
use Stringable;

use function get_debug_type;
use function is_array;
use function is_object;
use function is_string;
use function sprintf;
use function str_contains;

final class Javascript implements JavascriptInterface, JsonSerializable, Type, Stringable
{
    public readonly ?object $scope;

    public function __construct(public readonly string $code, array|object|null $scope = null)
    {
        if (str_contains($code, "\0")) {
            throw new InvalidArgumentException('Code cannot contain null bytes');
        }

        if ($scope instanceof PackedArray) {
            throw new UnexpectedValueException(
                'MongoDB\BSON\PackedArray cannot be serialized as a root document',
            );
        }

        if (is_array($scope)) {
            $this->scope = (object) $scope;
        } else {
            $this->scope = $scope;
        }
    }

    public function __debugInfo(): array
    {
        return [
            'code'  => $this->code,
            'scope' => $this->scope,
        ];
    }

    public function __serialize(): array
    {
        return [
            'code'  => $this->code,
            'scope' => $this->scope,
        ];
    }

    public static function __set_state(array $properties): static
    {
        if (! is_string($properties['code'] ?? null)) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Javascript initialization requires "code" string field',
            );
        }

        $scope = $properties['scope'] ?? null;

        if ($scope !== null && ! is_array($scope) && ! is_object($scope)) {
            throw new InvalidArgumentException(
                sprintf('Expected scope to be array or object, %s given', get_debug_type($scope)),
            );
        }

        if (str_contains($properties['code'], "\0")) {
            throw new InvalidArgumentException('Code cannot contain null bytes');
        }

        return new static($properties['code'], $scope);
    }

    public function __toString(): string
    {
        return $this->code;
    }

    public function __unserialize(array $data): void
    {
        if (! is_string($data['code'] ?? null)) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Javascript initialization requires "code" string field',
            );
        }

        $scope = $data['scope'] ?? null;

        if ($scope !== null && ! is_array($scope) && ! is_object($scope)) {
            throw new InvalidArgumentException(
                sprintf('Expected scope to be array or object, %s given', get_debug_type($scope)),
            );
        }

        if (str_contains($data['code'], "\0")) {
            throw new InvalidArgumentException('Code cannot contain null bytes');
        }

        $this->code  = $data['code'];
        $this->scope = is_array($scope) ? (object) $scope : $scope;
    }

    public function getCode(): string
    {
        return $this->code;
    }

    public function getScope(): ?object
    {
        return $this->scope;
    }

    public function jsonSerialize(): mixed
    {
        if ($this->scope === null) {
            return ['$code' => $this->code];
        }

        return [
            '$code'  => $this->code,
            '$scope' => $this->scope,
        ];
    }
}
