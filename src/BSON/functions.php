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

namespace MongoDB\BSON;

use function function_exists;

/* The extension declares these functions in its 1.x versions. The guard keeps
 * the library loadable when such an extension is present. */
if (! function_exists(__NAMESPACE__ . '\fromJSON')) {
    /**
     * Returns the BSON representation of a JSON value.
     *
     * @deprecated Use Document::fromJSON() or PackedArray::fromJSON() instead.
     */
    function fromJSON(string $json): string
    {
        return (string) Document::fromJSON($json);
    }

    /**
     * Returns the BSON representation of a PHP value.
     *
     * @deprecated Use Document::fromPHP() instead.
     */
    function fromPHP(array|object $value): string
    {
        return (string) Document::fromPHP($value);
    }

    /**
     * Returns the Canonical Extended JSON representation of a BSON value.
     *
     * @deprecated Use Document::toCanonicalExtendedJSON() instead.
     */
    function toCanonicalExtendedJSON(string $bson): string
    {
        return Document::fromBSON($bson)->toCanonicalExtendedJSON();
    }

    /**
     * Returns the Legacy Extended JSON representation of a BSON value.
     *
     * @deprecated Use Document::toCanonicalExtendedJSON() instead.
     */
    function toJSON(string $bson): string
    {
        return Document::fromBSON($bson)->toCanonicalExtendedJSON();
    }

    /**
     * Returns the PHP representation of a BSON value.
     *
     * @deprecated Use Document::toPHP() instead.
     *
     * @param array|null $typeMap
     */
    function toPHP(string $bson, ?array $typeMap = null): array|object
    {
        return Document::fromBSON($bson)->toPHP($typeMap);
    }

    /**
     * Returns the Relaxed Extended JSON representation of a BSON value.
     *
     * @deprecated Use Document::toRelaxedExtendedJSON() instead.
     */
    function toRelaxedExtendedJSON(string $bson): string
    {
        return Document::fromBSON($bson)->toRelaxedExtendedJSON();
    }
}
