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
 * Subscriber interface for Connection Monitoring and Pooling (CMAP) events.
 *
 * Implement this interface and register the instance via
 * {@see \MongoDB\Driver\Monitoring\addSubscriber()} or
 * {@see \MongoDB\Driver\Manager::addSubscriber()} to receive notifications
 * about connection pool lifecycle events.
 */
interface ConnectionPoolSubscriber extends Subscriber
{
    public function connectionCheckedIn(ConnectionCheckedInEvent $event): void;

    public function connectionCheckedOut(ConnectionCheckedOutEvent $event): void;

    public function connectionCheckOutFailed(ConnectionCheckOutFailedEvent $event): void;

    public function connectionCheckOutStarted(ConnectionCheckOutStartedEvent $event): void;

    public function connectionClosed(ConnectionClosedEvent $event): void;

    public function connectionCreated(ConnectionCreatedEvent $event): void;

    public function connectionPoolCleared(ConnectionPoolClearedEvent $event): void;

    public function connectionPoolClosed(ConnectionPoolClosedEvent $event): void;

    public function connectionPoolCreated(ConnectionPoolCreatedEvent $event): void;

    public function connectionPoolReady(ConnectionPoolReadyEvent $event): void;

    public function connectionReady(ConnectionReadyEvent $event): void;
}
