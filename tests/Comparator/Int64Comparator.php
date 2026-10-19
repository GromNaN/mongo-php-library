<?php

namespace MongoDB\Tests\Comparator;

use MongoDB\BSON\Int64;
use SebastianBergmann\Comparator\Comparator;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Exporter\Exporter;

use function is_float;
use function is_int;
use function is_numeric;
use function is_string;
use function sprintf;

class Int64Comparator extends Comparator
{
    public function accepts($expected, $actual): bool
    {
        // Only compare if either value is an Int64 and the other value is numeric
        return ($expected instanceof Int64 && $this->isComparable($actual))
            || ($actual instanceof Int64 && $this->isComparable($expected));
    }

    public function assertEquals($expected, $actual, $delta = 0.0, $canonicalize = false, $ignoreCase = false): void
    {
        if ($this->isEqual($expected, $actual)) {
            return;
        }

        $exporter = new Exporter();

        throw new ComparisonFailure(
            $expected,
            $actual,
            '',
            '',
            sprintf(
                'Failed asserting that %s matches expected %s.',
                $exporter->export($actual),
                $exporter->export($expected),
            ),
        );
    }

    /**
     * Compare the numeric values instead of using the == operator.
     *
     * ext-mongodb registers a C comparison handler that makes Int64 equal to an
     * int, a float, or a numeric string of the same value. Without the extension
     * there is no such handler, and the operator returns false. Comparing the
     * numeric values directly gives the same result in both modes.
     */
    private function isEqual($expected, $actual): bool
    {
        if ($expected instanceof Int64 && $actual instanceof Int64) {
            return (string) $expected === (string) $actual;
        }

        if ($expected instanceof Int64) {
            return $this->isEqualNumber((string) $expected, $actual);
        }

        if ($actual instanceof Int64) {
            return $this->isEqualNumber((string) $actual, $expected);
        }

        return $expected == $actual;
    }

    private function isEqualNumber(string $int64, $number): bool
    {
        if (is_int($number)) {
            return $int64 === (string) $number;
        }

        if (is_float($number)) {
            return (float) $int64 === $number;
        }

        if (is_string($number) && is_numeric($number)) {
            return (float) $int64 === (float) $number;
        }

        return false;
    }

    private function isComparable($value): bool
    {
        return $value instanceof Int64 || is_numeric($value);
    }
}
