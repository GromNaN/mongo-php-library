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

use Iterator;
use MongoDB\BSON\Int64;

/**
 * @template TValue of array|object
 * @template-extends Iterator<int, TValue>
 */
interface CursorInterface extends Iterator
{
    /**
     * @return TValue|null
     * @psalm-ignore-nullable-return
     */
    public function current(): array|object|null;

    public function getId(): Int64;

    public function getServer(): Server;

    public function isDead(): bool;

    /** @psalm-ignore-nullable-return */
    public function key(): ?int;

    public function setTypeMap(array $typemap): void;

    /** @return array<TValue> */
    public function toArray(): array;
}
