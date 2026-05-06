<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Expression;

use DateTimeInterface;
use MongoDB\BSON\Document;
use MongoDB\BSON\Serializable;
use MongoDB\BSON\Type;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\ExpressionInterface;
use MongoDB\Builder\Type\OperatorInterface;
use stdClass;

/**
 * Defines variables for use within the scope of a subexpression and returns the result of the subexpression. Accepts named parameters.
 * Accepts any number of argument expressions.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/let/
 * @internal
 * @psalm-type LetOperatorShape = array{vars: Document|Serializable|array|stdClass, in: DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null}|object{vars: Document|Serializable|array|stdClass, in: DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null}&stdClass|LetOperator
 */
final class LetOperator implements ResolvesToAny, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$let';
    public const PROPERTIES = ['vars' => 'vars', 'in' => 'in'];

    /**
     * @var Document|Serializable|array|stdClass $vars Assignment block for the variables accessible in the in expression. To assign a variable, specify a string for the variable name and assign a valid expression for the value.
     * The variable assignments have no meaning outside the in expression, not even within the vars block itself.
     */
    public readonly Document|Serializable|stdClass|array $vars;

    /** @var DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null $in The expression to evaluate. */
    public readonly DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|string|null $in;

    /**
     * @param Document|Serializable|array|stdClass $vars Assignment block for the variables accessible in the in expression. To assign a variable, specify a string for the variable name and assign a valid expression for the value.
     * The variable assignments have no meaning outside the in expression, not even within the vars block itself.
     * @param DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null $in The expression to evaluate.
     */
    public function __construct(
        Document|Serializable|stdClass|array $vars,
        DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|string|null $in,
    ) {
        $this->vars = $vars;
        $this->in = $in;
    }
}
