<?php

namespace MongoDB\Builder;

use LogicException;
use MongoDB\Builder\Expression\Expression;
use MongoDB\Builder\Expression\FieldPath;
use MongoDB\Builder\Expression\Variable;
use MongoDB\Builder\Query\OrQuery;
use MongoDB\Builder\Stage\GroupStage;
use MongoDB\Builder\Stage\ProjectStage;
use MongoDB\Builder\Stage\Stage;
use MongoDB\Codec\Codec;
use MongoDB\Codec\DecodeIfSupported;
use MongoDB\Codec\EncodeIfSupported;
use MongoDB\Exception\UnsupportedValueException;
use stdClass;

use function array_is_list;
use function array_merge;
use function is_array;
use function sprintf;

class BuilderCodec implements Codec
{
    use DecodeIfSupported;
    use EncodeIfSupported;

    /** The first property is the operator value */
    public const ENCODE_AS_SINGLE = 'single';

    /** Arguments as encoded as a map of properties to their values */
    public const ENCODE_AS_OBJECT = 'object';

    /** Properties are encoded as a list of values, names are ignored */
    public const ENCODE_AS_ARRAY = 'array';

    public function canDecode($value): false
    {
        return false;
    }

    public function canEncode($value): bool
    {
        return $value instanceof Pipeline || $value instanceof Stage || $value instanceof Expression;
    }

    public function decode($value)
    {
        throw UnsupportedValueException::invalidDecodableValue($value);
    }

    public function encode($value): array|stdClass|string|int|float|bool|null
    {
        if (! $this->canEncode($value)) {
            throw UnsupportedValueException::invalidEncodableValue($value);
        }

        // A pipeline is encoded as a list of stages
        if ($value instanceof Pipeline) {
            $encoded = [];
            foreach ($value->stages as $stage) {
                $encoded[] = $this->encodeIfSupported($stage);
            }

            return $encoded;
        }

        // This specific encoding code if temporary until we have a generic way to encode stages and operators
        if ($value instanceof FieldPath) {
            return '$' . $value->expression;
        }

        if ($value instanceof Variable) {
            return '$$' . $value->expression;
        }

        if ($value instanceof GroupStage) {
            $result = new stdClass();
            $result->_id = $this->encodeIfSupported($value->_id);
            // Specific: fields are encoded as a map of properties to their values at the top level as _id
            foreach ($value->fields as $key => $val) {
                $result->{$key} = $this->encodeIfSupported($val);
            }

            return (object) [$value::NAME => $result];
        }

        if ($value instanceof ProjectStage) {
            $result = new stdClass();
            // Specific: fields are encoded as a map of properties to their values at the top level as _id
            foreach ($value->specifications as $key => $val) {
                $result->{$key} = $this->encodeIfSupported($val);
            }

            return (object) [$value::NAME => $result];
        }

        if ($value instanceof OrQuery) {
            $result = [];
            foreach ($value->query as $query) {
                $encodedQuery = new stdClass();
                foreach ($query as $field => $expression) {
                    // Specific: $or queries are encoded as a list of expressions
                    // We need to merge query expressions into a single object
                    if (is_array($expression) && array_is_list($expression)) {
                        $mergedExpressions = [];
                        foreach ($expression as $expr) {
                            $mergedExpressions = array_merge($mergedExpressions, (array) $this->encodeIfSupported($expr));
                        }

                        $encodedQuery->{$field} = (object) $mergedExpressions;
                    } else {
                        $encodedQuery->{$field} = $this->encodeIfSupported($expression);
                    }
                }

                $result[] = $encodedQuery;
            }

            return (object) [$value::NAME => $result];
        }

        // The generic but incomplete encoding code
        switch ($value::ENCODE) {
            case self::ENCODE_AS_SINGLE:
                return $this->encodeAsSingle($value);

            case self::ENCODE_AS_ARRAY:
                return $this->encodeAsArray($value);

            case self::ENCODE_AS_OBJECT:
                return $this->encodeAsObject($value);
        }

        throw new LogicException(sprintf('Class "%s" does not have a valid ENCODE constant.', $value::class));
    }

    private function encodeAsSingle($value): stdClass
    {
        $result = [];
        foreach ($value as $val) {
            $result = $this->encodeIfSupported($val);
            break;
        }

        return (object) [$value::NAME => $result];
    }

    private function encodeAsArray($value): stdClass
    {
        $result = [];
        foreach ($value as $val) {
            $result[] = $this->encodeIfSupported($val);
        }

        return (object) [$value::NAME => $result];
    }

    private function encodeAsObject($value): stdClass
    {
        $result = new stdClass();
        foreach ($value as $key => $val) {
            $val = $this->encodeIfSupported($val);
            if ($val !== null) {
                $result->{$key} = $val;
            }
        }

        return (object) [$value::NAME => $result];
    }
}
