<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

namespace MongoDB\Builder\Stage;

use MongoDB\BSON\Document;
use MongoDB\BSON\Serializable;
use MongoDB\Builder\Expression\ExpressionInterface;
use MongoDB\Builder\Expression\ResolvesToObject;

class GroupStage implements StageInterface
{
    public const NAME = '$group';
    public const ENCODE = 'object';

    public mixed $_id;
    public array|null|object $fields;

    /**
     * @param ExpressionInterface|mixed $_id
     * @param Document|ResolvesToObject|Serializable|array|null|object $fields
     */
    public function __construct(mixed $_id, array|null|object $fields = null)
    {
        $this->_id = $_id;
        $this->fields = $fields;
    }
}
