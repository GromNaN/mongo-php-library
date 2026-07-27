<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Stage;

use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\StageInterface;

/**
 * Skips the first n documents where n is the specified skip number and passes the remaining documents unmodified to the pipeline. For each input document, outputs either zero documents (for the first n documents) or one document (if after the first n documents).
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/skip/
 * @internal
 */
final class SkipStage implements StageInterface, OperatorInterface
{
    public const ENCODE = Encode::Single;
    public const NAME = '$skip';
    public const PROPERTIES = ['skip' => 'skip'];

    /** @var int $skip */
    public readonly int $skip;

    /** @param int $skip */
    public function __construct(int $skip)
    {
        $this->skip = $skip;
    }

    /**
     * Executes the $skip stage locally on the provided documents.
     * Only for test/local execution purposes.
     *
     * @param array $documents Input documents
     * @return array Skipped documents
     */
    public function processLocally(array $documents): array
    {
        return array_slice($documents, $this->skip);
    }
}
