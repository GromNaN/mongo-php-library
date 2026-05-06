<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Stage;

use MongoDB\BSON\Document;
use MongoDB\BSON\Serializable;
use MongoDB\Builder\Stage\Type\RankFusionCombination;
use MongoDB\Builder\Stage\Type\RankFusionInput;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\Optional;
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
 * @psalm-import-type RankFusionInputShape from RankFusionInput
 * @psalm-import-type RankFusionCombinationShape from RankFusionCombination
 * @psalm-type RankFusionStageShape = array{input: RankFusionInputShape, scoreDetails?: bool, combination?: RankFusionCombinationShape}|object{input: RankFusionInputShape, scoreDetails?: bool, combination?: RankFusionCombinationShape}&stdClass|RankFusionStage
 */
final class RankFusionStage implements StageInterface, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$rankFusion';
    public const PROPERTIES = ['input' => 'input', 'scoreDetails' => 'scoreDetails', 'combination' => 'combination'];

    /** @var RankFusionInputShape $input An object that specifies the pipelines to combine with rank fusion. */
    public readonly RankFusionInput|Document|Serializable|stdClass|array $input;

    /** @var bool $scoreDetails Set to true to include detailed scoring information. */
    public readonly bool $scoreDetails;

    /** @var Optional|RankFusionCombinationShape $combination An object that specifies how to combine the ranked results. */
    public readonly Optional|RankFusionCombination|Document|Serializable|stdClass|array $combination;

    /**
     * @param RankFusionInputShape $input An object that specifies the pipelines to combine with rank fusion.
     * @param bool $scoreDetails Set to true to include detailed scoring information.
     * @param Optional|RankFusionCombinationShape $combination An object that specifies how to combine the ranked results.
     */
    public function __construct(
        RankFusionInput|Document|Serializable|stdClass|array $input,
        bool $scoreDetails = false,
        Optional|RankFusionCombination|Document|Serializable|stdClass|array $combination = Optional::Undefined,
    ) {
        if (is_array($input) || $input instanceof stdClass) {
            $input = new RankFusionInput(...(array) $input);
        }

        $this->input = $input;
        $this->scoreDetails = $scoreDetails;
        if (is_array($combination) || $combination instanceof stdClass) {
            $combination = new RankFusionCombination(...(array) $combination);
        }

        $this->combination = $combination;
    }
}
