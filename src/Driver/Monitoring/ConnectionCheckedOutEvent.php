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

namespace MongoDB\Driver\Monitoring;

/**
 * Event emitted when a connection is successfully checked out of the pool.
 */
final class ConnectionCheckedOutEvent
{
    public readonly string $address;

    /** @param int $durationMicros Duration from checkout start to connection ready, in microseconds. */
    public function __construct(
        public readonly int $connectionId,
        public readonly string $host,
        public readonly int $port,
        public readonly int $durationMicros,
    ) {
        $this->address = $host . ':' . $port;
    }
}
