<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Stage;

use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\QueryInterface;
use MongoDB\Builder\Type\QueryObject;
use MongoDB\Builder\Type\StageInterface;

use function is_array;

/**
 * Filters the document stream to allow only matching documents to pass unmodified into the next pipeline stage. $match uses standard MongoDB queries. For each input document, outputs either one document (a match) or zero documents (no match).
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/
 * @internal
 */
final class MatchStage implements StageInterface, OperatorInterface
{
    public const ENCODE = Encode::Single;
    public const NAME = '$match';
    public const PROPERTIES = ['query' => 'query'];

    /** @var QueryInterface|array $query */
    public readonly QueryInterface|array $query;

    /** @param QueryInterface|array $query */
    public function __construct(QueryInterface|array $query)
    {
        if (is_array($query)) {
            $query = QueryObject::create($query);
        }

        $this->query = $query;
    }

    /**
     * Executes the $match stage locally on the provided documents.
     * Only for test/local execution purposes.
     *
     * @param array $documents Input documents
     * @return array Filtered documents
     */
    public function processLocally(array $documents): array
    {
        // Safely convert query to array for local matching
        if (is_array($this->query)) {
            $query = $this->query;
        } elseif ($this->query instanceof \MongoDB\Builder\Type\QueryObject) {
            $query = $this->query->queries;
        } elseif (method_exists($this->query, 'getArrayCopy')) {
            $query = $this->query->getArrayCopy();
        } else {
            $query = [];
        }
        return array_values(array_filter($documents, function ($doc) use ($query) {
            foreach ($query as $field => $value) {
                if (!isset($doc[$field]) || $doc[$field] !== $value) {
                    return false;
                }
            }
            return true;
        }));
    }
}
