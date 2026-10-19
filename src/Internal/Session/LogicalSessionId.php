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

namespace MongoDB\Internal\Session;

use MongoDB\BSON\Binary;
use MongoDB\BSON\Serializable;

use function random_bytes;

/**
 * Logical session identifier.
 *
 * Wraps the UUID v4 Binary required by the sessions spec. Implementing
 * BSON\Serializable lets the encoder call bsonSerialize() directly instead of
 * reflecting over a generic object.
 *
 * @internal
 */
final class LogicalSessionId implements Serializable
{
    public function __construct(public readonly Binary $id)
    {
    }

    /** @return array{id: Binary} */
    public function bsonSerialize(): array
    {
        return ['id' => $this->id];
    }

    public static function generate(): self
    {
        return new self(new Binary(random_bytes(16), Binary::TYPE_UUID));
    }
}
