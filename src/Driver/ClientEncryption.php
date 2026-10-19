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

use MongoDB\BSON\Binary;
use MongoDB\Driver\Exception\RuntimeException;

final class ClientEncryption
{
    public const AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC = 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic';
    public const AEAD_AES_256_CBC_HMAC_SHA_512_RANDOM        = 'AEAD_AES_256_CBC_HMAC_SHA_512-Random';
    public const ALGORITHM_INDEXED                           = 'Indexed';
    public const ALGORITHM_UNINDEXED                         = 'Unindexed';
    public const ALGORITHM_RANGE                             = 'Range';
    public const QUERY_TYPE_EQUALITY                         = 'equality';
    public const QUERY_TYPE_RANGE                            = 'range';

    public function __construct(array $options)
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function addKeyAltName(Binary $keyId, string $keyAltName): ?object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function createDataKey(string $kmsProvider, ?array $options = null): Binary
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function decrypt(Binary $value): mixed
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function deleteKey(Binary $keyId): object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function encrypt(mixed $value, ?array $options = null): Binary
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function encryptExpression(array|object $expr, ?array $options = null): object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function getKey(Binary $keyId): ?object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function getKeyByAltName(string $keyAltName): ?object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function getKeys(): Cursor
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function removeKeyAltName(Binary $keyId, string $keyAltName): ?object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }

    public function rewrapManyDataKey(array|object $filter, ?array $options = null): object
    {
        throw new RuntimeException('Client-side encryption is not supported in this driver');
    }
}
