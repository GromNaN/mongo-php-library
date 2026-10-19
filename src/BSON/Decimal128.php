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

use JsonSerializable;
use MongoDB\Driver\Exception\InvalidArgumentException;
use Stringable;

use function ctype_digit;
use function is_string;
use function preg_match;
use function rtrim;
use function sprintf;
use function strlen;
use function strspn;

final class Decimal128 implements Decimal128Interface, JsonSerializable, Type, Stringable
{
    public readonly string $dec;

    public function __construct(string $value)
    {
        $this->dec = self::normalizeAndValidate($value);
    }

    public function __debugInfo(): array
    {
        return ['dec' => $this->dec];
    }

    public function __serialize(): array
    {
        return ['dec' => $this->dec];
    }

    public static function __set_state(array $properties): static
    {
        if (! is_string($properties['dec'] ?? null)) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Decimal128 initialization requires "dec" string field',
            );
        }

        return new static($properties['dec']);
    }

    public function __toString(): string
    {
        return $this->dec;
    }

    public function __unserialize(array $data): void
    {
        if (! is_string($data['dec'] ?? null)) {
            throw new InvalidArgumentException(
                'MongoDB\BSON\Decimal128 initialization requires "dec" string field',
            );
        }

        $this->dec = self::normalizeAndValidate($data['dec']);
    }

    public function jsonSerialize(): mixed
    {
        return ['$numberDecimal' => $this->dec];
    }

    private static function normalizeAndValidate(string $value): string
    {
        // Fast path: plain unsigned integer ≤ 34 digits — no regex needed.
        // sigDigits ≤ strlen ≤ 34, stored exponent = 0 ∈ [-6176, 6111].
        if (strlen($value) <= 34 && ctype_digit($value)) {
            return $value;
        }

        // Single regex handles sign, inf, nan, and numeric forms.
        if (
            ! preg_match(
                '/^
                    ([+-]?)                 # [1] optional leading sign
                    (?:
                        (inf(?:inity)?)     # [2] infinity keyword (case-insensitive)
                      | (nan)               # [3] NaN keyword (case-insensitive)
                      | (\d*)               # [4] integer digits of mantissa (may be empty with leading dot)
                        (?:\.(\d*))?        # [5] fractional digits after the decimal point
                        (?:[eE]([+-]?\d+))? # [6] signed exponent digits after E
                    )
                $/ix',
                $value,
                $m,
            )
        ) {
            throw new InvalidArgumentException(
                sprintf('Error parsing Decimal128 string: %s', $value),
            );
        }

        if ($m[2] !== '') {
            return $m[1] === '-' ? '-Infinity' : 'Infinity';
        }

        if ($m[3] !== '') {
            return 'NaN';
        }

        $intPart   = $m[4];
        $fracPart  = $m[5] ?? '';
        $parsedExp = (int) ($m[6] ?? 0);

        // Reject strings with no digits at all (e.g. bare "+" or ".")
        if ($intPart === '' && $fracPart === '') {
            throw new InvalidArgumentException(
                sprintf('Error parsing Decimal128 string: %s', $value),
            );
        }

        // Validate coefficient size and exponent range for Decimal128 (34 digits, exp -6176..6111).
        $intLen  = strlen($intPart);
        $fracLen = strlen($fracPart);

        // Leading zeros of the coefficient (intPart . fracPart) without string concatenation.
        $leadZeros = strspn($intPart, '0');
        if ($leadZeros === $intLen) {
            $leadZeros += strspn($fracPart, '0');
        }

        $coeffLen = $intLen + $fracLen - $leadZeros;

        if ($coeffLen > 0) {
            // Trailing zeros: count from end of fracPart, then intPart if all of fracPart is zeros.
            if ($fracLen > 0) {
                $rtrimFracLen  = strlen(rtrim($fracPart, '0'));
                $trailingZeros = $fracLen - $rtrimFracLen;
                if ($rtrimFracLen === 0) {
                    $trailingZeros += $intLen - strlen(rtrim($intPart, '0'));
                }
            } else {
                $trailingZeros = $intLen - strlen(rtrim($intPart, '0'));
            }

            $sigDigits = $coeffLen - $trailingZeros;

            // Too many significant digits to fit in a 34-digit coefficient.
            if ($sigDigits > 34) {
                throw new InvalidArgumentException(
                    sprintf('Error parsing Decimal128 string: %s', $value),
                );
            }

            // Overflow: even filling the coefficient to 34 digits, the stored exponent exceeds 6111.
            // E_min_achievable = parsedExp - fracLen + coeffLen - 34
            if ($parsedExp - $fracLen + $coeffLen - 34 > 6111) {
                throw new InvalidArgumentException(
                    sprintf('Error parsing Decimal128 string: %s', $value),
                );
            }

            // Underflow: even removing all trailing zeros, the stored exponent is below -6176.
            // E_max_achievable = parsedExp - fracLen + trailingZeros
            if ($parsedExp - $fracLen + $trailingZeros < -6176) {
                throw new InvalidArgumentException(
                    sprintf('Error parsing Decimal128 string: %s', $value),
                );
            }
        }

        return $value;
    }
}
