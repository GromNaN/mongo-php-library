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
use MongoDB\Driver\ServerDescription;
use MongoDB\Driver\TopologyDescription;

final class TopologyChangedEvent
{
    public readonly TopologyDescription $newDescription;
    public readonly TopologyDescription $previousDescription;

    /**
     * @param list<ServerDescription> $previousServers
     * @param list<ServerDescription> $newServers
     */
    private function __construct(
        public readonly ObjectId $topologyId,
        private readonly string $previousTopologyType,
        private readonly string $newTopologyType,
        array $previousServers = [],
        array $newServers = [],
    ) {
        $this->previousDescription = TopologyDescription::createFromInternal($previousTopologyType, $previousServers);
        $this->newDescription      = TopologyDescription::createFromInternal($newTopologyType, $newServers);
    }

    public function __debugInfo(): array
    {
        return [
            'topologyId'     => $this->topologyId,
            'newDescription' => $this->newDescription,
            'oldDescription' => $this->previousDescription,
        ];
    }

    public function getNewDescription(): TopologyDescription
    {
        return $this->newDescription;
    }

    public function getPreviousDescription(): TopologyDescription
    {
        return $this->previousDescription;
    }

    public function getTopologyId(): ObjectId
    {
        return $this->topologyId;
    }

    /**
     * @internal
     *
     * @param list<ServerDescription> $previousServers
     * @param list<ServerDescription> $newServers
     */
    public static function create(ObjectId $topologyId, string $previousTopologyType, string $newTopologyType, array $previousServers = [], array $newServers = []): self
    {
        return new self($topologyId, $previousTopologyType, $newTopologyType, $previousServers, $newServers);
    }

    /** @internal */
    public function getNewTopologyType(): string
    {
        return $this->newTopologyType;
    }

    /** @internal */
    public function getPreviousTopologyType(): string
    {
        return $this->previousTopologyType;
    }
}
