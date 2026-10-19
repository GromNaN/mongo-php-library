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
use Throwable;

final class CommandFailedEvent
{
    private function __construct(
        public readonly string $commandName,
        public readonly string $databaseName,
        public readonly Throwable $error,
        public readonly string $requestId,
        public readonly string $operationId,
        public readonly int $duration,
        public readonly object $reply,
        public readonly string $host = '',
        public readonly int $port = 27017,
        public readonly ?ObjectId $serviceId = null,
        public readonly ?int $serverConnectionId = null,
    ) {
    }

    public function __debugInfo(): array
    {
        return [
            'host'               => $this->host,
            'port'               => $this->port,
            'commandName'        => $this->commandName,
            'durationMicros'     => $this->duration,
            'error'              => $this->error,
            'reply'              => $this->reply,
            'operationId'        => $this->operationId,
            'requestId'          => $this->requestId,
            'serviceId'          => $this->serviceId,
            'serverConnectionId' => $this->serverConnectionId,
        ];
    }

    public function getCommandName(): string
    {
        return $this->commandName;
    }

    public function getDatabaseName(): string
    {
        return $this->databaseName;
    }

    public function getDurationMicros(): int
    {
        return $this->duration;
    }

    public function getError(): Throwable
    {
        return $this->error;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getOperationId(): string
    {
        return $this->operationId;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getReply(): object
    {
        return $this->reply;
    }

    public function getRequestId(): string
    {
        return $this->requestId;
    }

    public function getServerConnectionId(): ?int
    {
        return $this->serverConnectionId;
    }

    public function getServiceId(): ?ObjectId
    {
        return $this->serviceId;
    }

    /** @internal */
    public static function create(
        string $commandName,
        string $databaseName,
        Throwable $error,
        int $requestId,
        int $operationId,
        int $durationMicros,
        ?object $reply = null,
        string $host = '',
        int $port = 27017,
        ?ObjectId $serviceId = null,
        ?int $serverConnectionId = null,
    ): self {
        return new self($commandName, $databaseName, $error, (string) $requestId, (string) $operationId, $durationMicros, $reply ?? (object) [], $host, $port, $serviceId, $serverConnectionId);
    }
}
