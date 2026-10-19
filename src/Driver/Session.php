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

use Closure;
use MongoDB\BSON\Timestamp;
use MongoDB\BSON\TimestampInterface;
use MongoDB\Driver\Exception\LogicException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Internal\Session\LogicalSessionId;

/**
 * A client session.
 *
 * This prototype implements the parts of the Session API that the high level
 * library uses for CRUD operations: the logical session identifier attached to
 * commands, and the cluster and operation times. Multi-document transactions
 * are not implemented and the transaction methods throw.
 *
 * @see https://php.net/manual/en/class.mongodb-driver-session.php
 */
final class Session
{
    public const TRANSACTION_NONE = 'none';
    public const TRANSACTION_STARTING = 'starting';
    public const TRANSACTION_IN_PROGRESS = 'in-progress';
    public const TRANSACTION_COMMITTED = 'committed';
    public const TRANSACTION_ABORTED = 'aborted';

    private readonly LogicalSessionId $logicalSessionId;
    private string $transactionState = self::TRANSACTION_NONE;
    private bool $ended = false;
    private ?object $clusterTime = null;
    private ?Timestamp $operationTime = null;

    /** @param Closure(): Server $serverProvider */
    private function __construct(
        private readonly Closure $serverProvider,
        private readonly array $options = [],
    ) {
        $this->logicalSessionId = LogicalSessionId::generate();
    }

    public function __debugInfo(): array
    {
        return [
            'logicalSessionId' => $this->logicalSessionId,
            'clusterTime' => $this->clusterTime,
            'operationTime' => $this->operationTime,
            'transactionState' => $this->transactionState,
            'isDirty' => $this->isDirty(),
        ];
    }

    public function abortTransaction(): void
    {
        throw new RuntimeException('Transactions are not supported by the decoupling prototype');
    }

    public function commitTransaction(): void
    {
        throw new RuntimeException('Transactions are not supported by the decoupling prototype');
    }

    public function endSession(): void
    {
        $this->ended = true;
        $this->transactionState = self::TRANSACTION_NONE;
    }

    public function getClusterTime(): ?object
    {
        return $this->clusterTime;
    }

    public function getLogicalSessionId(): object
    {
        return $this->logicalSessionId;
    }

    public function getOperationTime(): ?Timestamp
    {
        return $this->operationTime;
    }

    public function getServer(): ?Server
    {
        return ($this->serverProvider)();
    }

    public function getTransactionOptions(): ?array
    {
        return $this->options === [] ? null : $this->options;
    }

    public function getTransactionState(): string
    {
        return $this->transactionState;
    }

    public function isDirty(): bool
    {
        return false;
    }

    public function isInTransaction(): bool
    {
        return $this->transactionState !== self::TRANSACTION_NONE
            && $this->transactionState !== self::TRANSACTION_COMMITTED
            && $this->transactionState !== self::TRANSACTION_ABORTED;
    }

    public function startTransaction(?array $options = null): void
    {
        if ($this->ended) {
            throw new LogicException('Cannot start a transaction on an ended session');
        }

        throw new RuntimeException('Transactions are not supported by the decoupling prototype');
    }

    /** @internal */
    public function advanceClusterTime(array|object $clusterTime): void
    {
        $this->clusterTime = (object) $clusterTime;
    }

    /** @internal */
    public function advanceOperationTime(TimestampInterface $operationTime): void
    {
        $this->operationTime = $operationTime instanceof Timestamp
            ? $operationTime
            : new Timestamp($operationTime->getIncrement(), $operationTime->getTimestamp());
    }

    /**
     * Create a session.
     *
     * The extension creates sessions from the manager, so the constructor is not
     * part of its API. This factory is the userland equivalent.
     *
     * @internal Sessions are created by Manager::startSession().
     *
     * @param Closure(): Server $serverProvider
     */
    public static function create(Closure $serverProvider, array $options = []): self
    {
        return new self($serverProvider, $options);
    }

    /** @internal Marks that the startTransaction flag was sent on the wire. */
    public function startTransactionSent(): void
    {
        $this->transactionState = self::TRANSACTION_IN_PROGRESS;
    }
}
