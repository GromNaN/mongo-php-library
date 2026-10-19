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

interface SDAMSubscriber extends Subscriber
{
    public function serverChanged(ServerChangedEvent $event): void;

    public function serverClosed(ServerClosedEvent $event): void;

    public function serverHeartbeatFailed(ServerHeartbeatFailedEvent $event): void;

    public function serverHeartbeatStarted(ServerHeartbeatStartedEvent $event): void;

    public function serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent $event): void;

    public function serverOpening(ServerOpeningEvent $event): void;

    public function topologyChanged(TopologyChangedEvent $event): void;

    public function topologyClosed(TopologyClosedEvent $event): void;

    public function topologyOpening(TopologyOpeningEvent $event): void;
}
