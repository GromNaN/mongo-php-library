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

use MongoDB\BSON\PackedArray;

use function function_exists;
use function is_array;
use function is_object;

/* The extension declares this function in its 1.x versions. The guard keeps the
 * library loadable when such an extension is present. */
if (! function_exists(__NAMESPACE__ . '\is_document')) {
    /**
     * Returns true when $value can be used as a BSON root document.
     *
     * A valid root document is an array or any object except PackedArray,
     * which represents a BSON array and cannot be used at the root level.
     */
    function is_document(mixed $value): bool
    {
        return is_array($value) || (is_object($value) && ! $value instanceof PackedArray);
    }
}
