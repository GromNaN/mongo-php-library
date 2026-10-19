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

final class ServerDescription
{
    public const TYPE_UNKNOWN = 'Unknown';
    public const TYPE_STANDALONE = 'Standalone';
    public const TYPE_MONGOS = 'Mongos';
    public const TYPE_POSSIBLE_PRIMARY = 'PossiblePrimary';
    public const TYPE_RS_PRIMARY = 'RSPrimary';
    public const TYPE_RS_SECONDARY = 'RSSecondary';
    public const TYPE_RS_ARBITER = 'RSArbiter';
    public const TYPE_RS_OTHER = 'RSOther';
    public const TYPE_RS_GHOST = 'RSGhost';
    public const TYPE_LOAD_BALANCER = 'LoadBalancer';

    private string $host;
    private int $port;
    private string $type;
    private array $helloResponse;
    private int $lastUpdateTime;
    private ?float $roundTripTime = null;

    /**
     * Private constructor. Use the internal factory to create instances.
     */
    private function __construct()
    {
    }

    public function __debugInfo(): array
    {
        return [
            'host'             => $this->host,
            'port'             => $this->port,
            'type'             => $this->type,
            'hello_response'   => $this->helloResponse,
            'last_update_time' => $this->lastUpdateTime,
            'round_trip_time'  => $this->roundTripTime !== null ? (int) $this->roundTripTime : null,
        ];
    }

    public function getHelloResponse(): array
    {
        return $this->helloResponse;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getLastUpdateTime(): int
    {
        return $this->lastUpdateTime;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getRoundTripTime(): ?int
    {
        return $this->roundTripTime !== null ? (int) $this->roundTripTime : null;
    }

    public function getType(): string
    {
        return $this->type;
    }

    /** @internal Creates a new ServerDescription instance. */
    public static function createFromInternal(
        string $host,
        int $port,
        string $type,
        ?float $roundTripTime,
        array $helloResponse,
        int $lastUpdateTime,
    ): static {
        $instance = new static();
        $instance->host             = $host;
        $instance->port             = $port;
        $instance->type             = $type;
        $instance->roundTripTime  = $roundTripTime;
        $instance->helloResponse   = $helloResponse;
        $instance->lastUpdateTime = $lastUpdateTime;

        return $instance;
    }
}
