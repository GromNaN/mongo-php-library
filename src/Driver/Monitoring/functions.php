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

/**
 * Bootstrap for the MongoDB userland driver.
 *
 * All classes are autoloaded via PSR-4 (see composer.json autoload section).
 * This file is only needed for global functions that cannot be autoloaded.
 */

namespace MongoDB\Driver\Monitoring;

use MongoDB\Internal\Monitoring\Dispatcher;

use function function_exists;

if (! function_exists(__NAMESPACE__ . '\addSubscriber')) {
    function addSubscriber(Subscriber $subscriber): void
    {
        Dispatcher::addGlobalSubscriber($subscriber);
    }

    function removeSubscriber(Subscriber $subscriber): void
    {
        Dispatcher::removeGlobalSubscriber($subscriber);
    }

    function mongoc_log(int $level, string $domain, string $message): void
    {
        Dispatcher::log($level, $domain, $message);
    }
}
