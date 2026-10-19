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

use MongoDB\BSON\ObjectId;

final class ServerClosedEvent
{
    private function __construct(
        public readonly string $host,
        public readonly int $port,
        public readonly ObjectId $topologyId,
    ) {
    }

    public function __debugInfo(): array
    {
        return [
            'host'       => $this->host,
            'port'       => $this->port,
            'topologyId' => $this->topologyId,
        ];
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getTopologyId(): ObjectId
    {
        return $this->topologyId;
    }

    /** @internal */
    public static function create(string $host, int $port, ObjectId $topologyId): self
    {
        return new self($host, $port, $topologyId);
    }
}
