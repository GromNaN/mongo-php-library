<?php

declare(strict_types=1);

namespace MongoDB\Builder\Accumulator;

use MongoDB\Builder\Type\AccumulatorInterface;
use MongoDB\Builder\Accumulator\SumAccumulator;
use MongoDB\Builder\Accumulator\MaxAccumulator;

/**
 * Implements windowed output for accumulators (sum, max, etc.)
 */
final class OutputWindowAccumulator
{
    public readonly AccumulatorInterface $accumulator;
    public readonly array $documents;

    public function __construct(AccumulatorInterface $accumulator, array $documents = [])
    {
        $this->accumulator = $accumulator;
        $this->documents = $documents;
    }

    /**
     * Accumulate the window for the given documents.
     *
     * @param array $docs Window documents
     * @return mixed
     */
    public function accumulateWindow(array $docs): mixed
    {
        if ($this->accumulator instanceof SumAccumulator) {
            $sum = 0;
            foreach ($docs as $doc) {
                $state = [];
                $this->accumulator->accumulate($state, $doc);
                $sum += $state['value'] ?? 0;
            }
            return $sum;
        }
        if ($this->accumulator instanceof MaxAccumulator) {
            $max = null;
            foreach ($docs as $doc) {
                $state = [];
                $this->accumulator->accumulate($state, $doc);
                if ($max === null || ($state['value'] ?? null) > $max) {
                    $max = $state['value'] ?? null;
                }
            }
            return $max;
        }
        // Add more accumulator support as needed
        return null;
    }
}

