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

namespace MongoDB\Internal\Connection;

use function json_encode;
use function php_uname;
use function strlen;

use const PHP_OS_FAMILY;
use const PHP_VERSION;

/**
 * Builds the `client` metadata document sent in the hello handshake.
 *
 * This is a reduced version of the Handshake spec implementation: the driver
 * name, version, OS type, and platform are reported, and the optional
 * application name is appended. FaaS and container detection is not
 * implemented, since it does not affect the CRUD path.
 *
 * @internal
 */
final class ClientMetadata
{
    private const DRIVER_NAME    = 'mongodb-php-library-prototype';
    private const DRIVER_VERSION = '0.1.0';
    private const MAX_SIZE       = 512;

    /** @param array{name?: string, version?: string, platform?: string} $driverInfo */
    public static function build(?string $appName, array $driverInfo): array
    {
        $name    = self::DRIVER_NAME;
        $version = self::DRIVER_VERSION;

        if (isset($driverInfo['name']) && $driverInfo['name'] !== '') {
            $name .= '/' . $driverInfo['name'];
        }

        if (isset($driverInfo['version']) && $driverInfo['version'] !== '') {
            $version .= '/' . $driverInfo['version'];
        }

        $metadata = [
            'driver' => ['name' => $name, 'version' => $version],
            'os' => ['type' => PHP_OS_FAMILY, 'name' => php_uname('s'), 'architecture' => php_uname('m')],
            'platform' => 'PHP ' . PHP_VERSION,
        ];

        if ($appName !== null && $appName !== '') {
            $metadata['application'] = ['name' => $appName];
        }

        if (isset($driverInfo['platform']) && $driverInfo['platform'] !== '') {
            $metadata['platform'] .= ' / ' . $driverInfo['platform'];
        }

        // The handshake spec caps the client document at 512 bytes. The
        // optional fields are dropped in reverse order of importance until the
        // document fits.
        if (strlen(json_encode($metadata)) > self::MAX_SIZE) {
            unset($metadata['application']);

            if (strlen(json_encode($metadata)) > self::MAX_SIZE) {
                unset($metadata['os']['name'], $metadata['os']['architecture']);
            }
        }

        return $metadata;
    }
}
