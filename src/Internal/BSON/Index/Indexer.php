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

namespace MongoDB\Internal\BSON\Index;

use InvalidArgumentException;
use MongoDB\Internal\BSON\BsonType;

use function preg_match;
use function sprintf;
use function str_ends_with;
use function strlen;
use function unpack;

/**
 * Scans raw BSON bytes and returns a flat list of field metadata records
 * (key name, type byte, and byte offsets/lengths for each field's data).
 * No PHP values are instantiated; decoding is deferred to Index\Field.
 *
 * @internal
 */
final class Indexer
{
    /** @return list<array{key: string, bsonType: int, keyOffset: int, keyLength: int, dataOffset: int|null, dataLength: int|null}> */
    public function getIndex(string $bson): array
    {
        $fields = [];

        $offset = 4;
        $length = $this->getBSONLength($bson);

        while ($offset < $length - 1) {
            [$data, $offset] = $this->getNextFieldData($bson, $offset);
            $fields[]        = $data;
        }

        return $fields;
    }

    private function getBSONLength(string $bson): int
    {
        $data = @unpack('V', $bson);
        if ($data === false) {
            throw new InvalidArgumentException('Invalid BSON data');
        }

        [1 => $length] = $data;
        if ($length < 5) {
            throw new InvalidArgumentException('Invalid BSON length');
        }

        if (strlen($bson) !== $length) {
            throw new InvalidArgumentException('Invalid BSON length');
        }

        if (! str_ends_with($bson, "\0")) {
            throw new InvalidArgumentException('Invalid BSON length');
        }

        return $length;
    }

    private function getNextFieldData(string $bson, int $offset): array
    {
        $data = @unpack('Ctype/Z*key', $bson, $offset);
        if ($data === false) {
            throw new InvalidArgumentException('Invalid BSON data');
        }

        ['type' => $bsonType, 'key' => $key] = $data;

        if (! preg_match('//u', $key)) {
            throw new InvalidArgumentException('Invalid UTF-8 data in BSON key');
        }

        // Shift offset by 1 byte for type, key length and a null byte
        $newOffset = $offset + 1 + strlen($key) + 1;

        switch ($bsonType) {
            case BsonType::DOUBLE:
                $dataOffset = $newOffset;
                $dataLength = 8;
                $newOffset += 8;
                break;

            case BsonType::STRING:
            case BsonType::CODE:
            case BsonType::SYMBOL:
                $data = @unpack('Vlength', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                // dataOffset points to the int32 length header (BsonDecoder convention)
                $dataOffset = $newOffset;
                $dataLength = 4 + (int) $data['length'];
                $newOffset += $dataLength;
                break;

            case BsonType::DOCUMENT:
            case BsonType::ARRAY:
                $data = @unpack('Vlength', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                $dataLength = (int) $data['length'];
                $dataOffset = $newOffset;
                $newOffset += $dataLength;
                break;

            case BsonType::BINARY:
                $data = @unpack('Vlength', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                // dataOffset points to the int32 length header; total = int32 + subtype byte + data
                $dataOffset = $newOffset;
                $dataLength = 4 + 1 + (int) $data['length'];
                $newOffset += $dataLength;
                break;

            case BsonType::UNDEFINED:
            case BsonType::NULL:
            case BsonType::MINKEY:
            case BsonType::MAXKEY:
                $dataLength = 0;
                $dataOffset = null;
                break;

            case BsonType::OBJECTID:
                // An ObjectId is always 12 bytes long
                $dataLength = 12;
                $dataOffset = $newOffset;
                $newOffset += $dataLength;
                break;

            case BsonType::BOOLEAN:
                $dataLength = 1;
                $dataOffset = $newOffset;
                $newOffset += $dataLength;
                break;

            case BsonType::DATE:
            case BsonType::TIMESTAMP:
            case BsonType::INT64:
                $dataLength = 8;
                $dataOffset = $newOffset;
                $newOffset += $dataLength;
                break;

            case BsonType::REGEX:
                $data = @unpack('Z*pattern', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                $dataOffset = $newOffset;
                $dataLength = strlen($data['pattern']) + 1;
                $newOffset += $dataLength;

                $data = @unpack('Z*options', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                // Since a regex contains two strings, the data will be both strings with their NUL bytes
                $dataLength += strlen($data['options']) + 1;
                $newOffset   = $dataOffset + $dataLength;
                break;

            case BsonType::DBPOINTER:
                // string (byte*12)
                $data = @unpack('Vlength', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                $dataOffset = $newOffset;
                // Data length includes 4 bytes for the string length and 12 bytes for an ObjectId
                $dataLength = 4 + (int) $data['length'] + 12;
                $newOffset += $dataLength;
                break;

            case BsonType::CODEWITHSCOPE:
                // int32 string document
                // The int32 contains the total number of bytes in the code_w_scope (including itself)
                $data = @unpack('Vlength', $bson, $newOffset);
                if ($data === false) {
                    throw new InvalidArgumentException('Invalid BSON data');
                }

                // dataOffset points to the outer int32 (BsonDecoder convention)
                $dataOffset = $newOffset;
                $dataLength = (int) $data['length'];
                $newOffset += $dataLength;
                break;

            case BsonType::INT32:
                $dataLength = 4;
                $dataOffset = $newOffset;
                $newOffset += $dataLength;
                break;

            case BsonType::DECIMAL128:
                $dataLength = 16;
                $dataOffset = $newOffset;
                $newOffset += $dataLength;
                break;

            default:
                throw new InvalidArgumentException(sprintf('Invalid BSON type "%d"', $bsonType));
        }

        return [
            [
                'key'        => $key,
                'bsonType'   => $bsonType,
                'keyOffset'  => $offset + 1,
                'keyLength'  => strlen($key),
                'dataOffset' => $dataOffset,
                'dataLength' => $dataLength,
            ],
            $newOffset,
        ];
    }
}
