<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Stage;

use DateTimeInterface;
use MongoDB\BSON\Document;
use MongoDB\BSON\Serializable;
use MongoDB\BSON\Type;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\ExpressionInterface;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\Optional;
use MongoDB\Builder\Type\StageInterface;
use stdClass;

/**
 * Groups documents into windows and applies one or more operators to the documents in each window.
 * New in MongoDB 5.0.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/setWindowFields/
 * @internal
 */
final class SetWindowFieldsStage implements StageInterface, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$setWindowFields';
    public const PROPERTIES = ['sortBy' => 'sortBy', 'output' => 'output', 'partitionBy' => 'partitionBy'];

    /** @var Document|Serializable|array|stdClass $sortBy Specifies the field(s) to sort the documents by in the partition. Uses the same syntax as the $sort stage. Default is no sorting. */
    public readonly Document|Serializable|stdClass|array $sortBy;

    /**
     * @var Document|Serializable|array|stdClass $output Specifies the field(s) to append to the documents in the output returned by the $setWindowFields stage. Each field is set to the result returned by the window operator.
     * A field can contain dots to specify embedded document fields and array fields. The semantics for the embedded document dotted notation in the $setWindowFields stage are the same as the $addFields and $set stages.
     */
    public readonly Document|Serializable|stdClass|array $output;

    /** @var Optional|DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|null|stdClass|string $partitionBy Specifies an expression to group the documents. In the $setWindowFields stage, the group of documents is known as a partition. Default is one partition for the entire collection. */
    public readonly Optional|DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|null|string $partitionBy;

    /**
     * @param Document|Serializable|array|stdClass $sortBy Specifies the field(s) to sort the documents by in the partition. Uses the same syntax as the $sort stage. Default is no sorting.
     * @param Document|Serializable|array|stdClass $output Specifies the field(s) to append to the documents in the output returned by the $setWindowFields stage. Each field is set to the result returned by the window operator.
     * A field can contain dots to specify embedded document fields and array fields. The semantics for the embedded document dotted notation in the $setWindowFields stage are the same as the $addFields and $set stages.
     * @param Optional|DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|null|stdClass|string $partitionBy Specifies an expression to group the documents. In the $setWindowFields stage, the group of documents is known as a partition. Default is one partition for the entire collection.
     */
    public function __construct(
        Document|Serializable|stdClass|array $sortBy,
        Document|Serializable|stdClass|array $output,
        Optional|DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|null|string $partitionBy = Optional::Undefined,
    ) {
        $this->sortBy = $sortBy;
        $this->output = $output;
        $this->partitionBy = $partitionBy;
    }

    /**
     * Executes the $setWindowFields stage locally on the provided documents.
     * Only for test/local execution purposes.
     * Supports partitioning, sorting, and window accumulators (sum, max).
     *
     * @param array $documents Input documents
     * @return array Documents with window fields
     */
    public function processLocally(array $documents): array
    {
        // Partition documents generically
        $partitions = [];
        foreach ($documents as $doc) {
            $key = null;
            if (is_callable($this->partitionBy)) {
                $key = ($this->partitionBy)($doc);
            } elseif (is_object($this->partitionBy) && method_exists($this->partitionBy, 'extract')) {
                $key = $this->partitionBy->extract($doc);
            } elseif (is_object($this->partitionBy) && method_exists($this->partitionBy, '__toString')) {
                $field = (string) $this->partitionBy;
                $key = $doc[$field] ?? null;
            } elseif (is_string($this->partitionBy)) {
                $key = $doc[$this->partitionBy] ?? null;
            } elseif ($this->partitionBy instanceof Optional && $this->partitionBy === Optional::Undefined) {
                $key = null;
            }
            $partitions[$key][] = $doc;
        }
        // Sort partitions
        $sortSpec = (array) $this->sortBy;
        foreach ($partitions as &$docs) {
            usort($docs, function ($a, $b) use ($sortSpec) {
                foreach ($sortSpec as $field => $direction) {
                    $dir = ($direction === -1 || $direction === 'desc' || $direction === \MongoDB\Builder\Type\Sort::Desc) ? -1 : 1;
                    $aVal = $a[$field] ?? null;
                    $bVal = $b[$field] ?? null;
                    if ($aVal === $bVal) {
                        continue;
                    }
                    return ($aVal < $bVal ? -1 : 1) * $dir;
                }
                return 0;
            });
        }
        // Apply window accumulators
        $outputSpec = (array) $this->output;
        $result = [];
        foreach ($partitions as $docs) {
            $count = count($docs);
            for ($i = 0; $i < $count; $i++) {
                $doc = $docs[$i];
                $newDoc = $doc;
                foreach ($outputSpec as $field => $windowOp) {
                    // Only support outputWindow for now
                    if (method_exists($windowOp, 'accumulateWindow')) {
                        // Determine window bounds
                        $windowDocs = [];
                        if (isset($windowOp->documents)) {
                            $bounds = $windowOp->documents;
                            // ['unbounded', 'current'] means from 0 to $i
                            $start = ($bounds[0] === 'unbounded') ? 0 : max(0, $i + $bounds[0]);
                            $end = ($bounds[1] === 'current') ? $i : (($bounds[1] === 'unbounded') ? $count - 1 : min($count - 1, $i + $bounds[1]));
                            for ($j = $start; $j <= $end; $j++) {
                                $windowDocs[] = $docs[$j];
                            }
                        } else {
                            $windowDocs = $docs;
                        }
                        $newDoc[$field] = $windowOp->accumulateWindow($windowDocs);
                    }
                }
                $result[] = $newDoc;
            }
        }
        return $result;
    }
}
