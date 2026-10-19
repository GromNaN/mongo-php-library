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

namespace MongoDB\Internal\BSON;

final class BsonType
{
    public const DOUBLE        = 1;
    public const STRING        = 2;
    public const DOCUMENT      = 3;
    public const ARRAY         = 4;
    public const BINARY        = 5;
    public const UNDEFINED     = 6;
    public const OBJECTID      = 7;
    public const BOOLEAN       = 8;
    public const DATE          = 9;
    public const NULL          = 10;
    public const REGEX         = 11;
    public const DBPOINTER     = 12;
    public const CODE          = 13;
    public const SYMBOL        = 14;
    public const CODEWITHSCOPE = 15;
    public const INT32         = 16;
    public const TIMESTAMP     = 17;
    public const INT64         = 18;
    public const DECIMAL128    = 19;
    public const MINKEY        = 255;
    public const MAXKEY        = 127;
}
