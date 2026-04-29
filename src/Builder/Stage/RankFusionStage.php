<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Stage;

use MongoDB\BSON\Document;
use MongoDB\BSON\Serializable;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\Optional;
use MongoDB\Builder\Type\RankFusionCombination;
use MongoDB\Builder\Type\RankFusionInput;
use MongoDB\Builder\Type\StageInterface;
use stdClass;

use function is_array;

/**
 * Combines multiple pipelines using rank-based fusion to create hybrid search results.
 *
 * New in MongoDB 8.1
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/rankFusion/
 * @internal
 */
final class RankFusionStage implements StageInterface, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$rankFusion';
    public const PROPERTIES = ['input' => 'input', 'scoreDetails' => 'scoreDetails', 'combination' => 'combination'];

    /** @var RankFusionInput|Document|Serializable|array|stdClass $input An object that specifies the pipelines to combine with rank fusion. */
    public readonly RankFusionInput|Document|Serializable|stdClass|array $input;

    /** @var bool $scoreDetails Set to true to include detailed scoring information. */
    public readonly bool $scoreDetails;

    /** @var Optional|RankFusionCombination|Document|Serializable|array|stdClass $combination An object that specifies how to combine the ranked results. */
    public readonly Optional|RankFusionCombination|Document|Serializable|stdClass|array $combination;

    /**
     * @param RankFusionInput|Document|Serializable|array|stdClass $input An object that specifies the pipelines to combine with rank fusion.
     * @param bool $scoreDetails Set to true to include detailed scoring information.
     * @param Optional|RankFusionCombination|Document|Serializable|array|stdClass $combination An object that specifies how to combine the ranked results.
     */
    public function __construct(
        RankFusionInput|Document|Serializable|stdClass|array $input,
        bool $scoreDetails = false,
        Optional|RankFusionCombination|Document|Serializable|stdClass|array $combination = Optional::Undefined,
    ) {
        if (is_array($input)) {
            $input = new RankFusionInput(...$input);
        }

        $this->input = $input;
        $this->scoreDetails = $scoreDetails;
        if (is_array($combination)) {
            $combination = new RankFusionCombination(...$combination);
        }

        $this->combination = $combination;
    }
}
