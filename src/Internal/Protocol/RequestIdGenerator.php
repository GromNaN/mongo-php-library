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

namespace MongoDB\Internal\Protocol;

use const PHP_INT_MAX;

/**
 * Thread-safe (single-process) monotonically increasing request-ID generator.
 *
 * @internal
 */
final class RequestIdGenerator
{
    private static int $counter = 0;

    /**
     * Return the next request ID, wrapping around to 1 when PHP_INT_MAX is reached.
     */
    public static function next(): int
    {
        if (self::$counter >= PHP_INT_MAX) {
            self::$counter = 0;
        }

        return ++self::$counter;
    }
}
