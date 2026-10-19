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

final class TopologyDescription
{
    public const TYPE_UNKNOWN                    = 'Unknown';
    public const TYPE_SINGLE                     = 'Single';
    public const TYPE_SHARDED                    = 'Sharded';
    public const TYPE_REPLICA_SET_NO_PRIMARY     = 'ReplicaSetNoPrimary';
    public const TYPE_REPLICA_SET_WITH_PRIMARY   = 'ReplicaSetWithPrimary';
    public const TYPE_LOAD_BALANCED              = 'LoadBalanced';

    /** @param list<ServerDescription> $servers */
    private function __construct(
        private readonly string $type,
        private array $servers = [],
    ) {
    }

    public function __debugInfo(): array
    {
        return [
            'servers' => $this->servers,
            'type'    => $this->type,
        ];
    }

    /** @return list<ServerDescription> */
    public function getServers(): array
    {
        return $this->servers;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function hasReadableServer(?ReadPreference $readPreference = null): bool
    {
        return $this->servers !== [];
    }

    public function hasWritableServer(): bool
    {
        return $this->servers !== [];
    }

    /**
     * @internal
     *
     * @param list<ServerDescription> $servers
     */
    public static function createFromInternal(string $type, array $servers = []): self
    {
        return new self($type, $servers);
    }
}
