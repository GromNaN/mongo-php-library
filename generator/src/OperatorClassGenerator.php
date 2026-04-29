<?php

declare(strict_types=1);

namespace MongoDB\CodeGenerator;

use MongoDB\BSON\Javascript;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\Optional;
use MongoDB\Builder\Type\QueryObject;
use MongoDB\Builder\Type\TypeInterface;
use MongoDB\CodeGenerator\Definition\ArgumentDefinition;
use MongoDB\CodeGenerator\Definition\GeneratorDefinition;
use MongoDB\CodeGenerator\Definition\OperatorDefinition;
use MongoDB\CodeGenerator\Definition\VariadicType;
use MongoDB\Exception\InvalidArgumentException;
use Nette\PhpGenerator\Literal;
use Nette\PhpGenerator\Method;
use Nette\PhpGenerator\Parameter;
use Nette\PhpGenerator\PhpNamespace;
use Nette\PhpGenerator\Property;
use RuntimeException;
use stdClass;
use Throwable;

use function assert;
use function interface_exists;
use function rtrim;
use function sprintf;
use function str_starts_with;
use function strlen;
use function substr;

/**
 * Generates a value object class for stages and operators.
 */
class OperatorClassGenerator extends OperatorGenerator
{
    public function generate(GeneratorDefinition $definition): void
    {
        foreach ($this->getOperators($definition) as $operator) {
            try {
                $this->writeFile($this->createClass($definition, $operator));
                foreach ($operator->arguments as $argument) {
                    if ($argument->arguments) {
                        $this->writeFile($this->createArgumentTypeClass($operator, $argument));
                    }
                }
            } catch (Throwable $e) {
                throw new RuntimeException(sprintf('Failed to generate class for operator "%s"', $operator->name), 0, $e);
            }
        }
    }

    public function createClass(GeneratorDefinition $definition, OperatorDefinition $operator): PhpNamespace
    {
        $namespace = new PhpNamespace($definition->namespace);

        $interfaces = $this->getInterfaces($operator);
        foreach ($interfaces as $interface) {
            $namespace->addUse($interface);
        }

        $class = $namespace->addClass($this->getOperatorClassName($definition, $operator));
        $class->setFinal();
        $class->setImplements($interfaces);
        $namespace->addUse(OperatorInterface::class);
        $class->addImplement(OperatorInterface::class);

        // Expose operator metadata as constants
        // @todo move to encoder class
        $class->addComment($operator->description);
        $class->addComment('@see ' . $operator->link);
        $class->addComment('@internal');
        $namespace->addUse(Encode::class);
        $class->addConstant('ENCODE', new Literal('Encode::' . $operator->encode->name));
        $class->addConstant('NAME', $operator->wrapObject ? $operator->name : null);

        $encodeNames = [];
        $constructor = $class->addMethod('__construct');
        foreach ($operator->arguments as $argument) {
            $encodeNames[$argument->propertyName] = $argument->mergeObject ? null : $argument->name;

            $type = $this->getAcceptedTypes($argument);

            // If the argument has sub-fields, inject the generated type class into the union
            $typeClassName = $this->getArgumentTypeClassName($operator, $argument);
            if ($typeClassName !== null) {
                $namespace->addUse('\\' . $typeClassName);
                $type->use[] = '\\' . $typeClassName;
                [, $typeShortName] = $this->splitNamespaceAndClassName($typeClassName);

                if ($argument->optional) {
                    // Insert the type class right after Optional
                    $optionalNative = '\\' . Optional::class . '|';
                    $optionalDoc = 'Optional|';
                    if (str_starts_with($type->native, $optionalNative)) {
                        $type->native = $optionalNative . '\\' . $typeClassName . '|' . substr($type->native, strlen($optionalNative));
                    }

                    if (str_starts_with($type->doc, $optionalDoc)) {
                        $type->doc = $optionalDoc . $typeShortName . '|' . substr($type->doc, strlen($optionalDoc));
                    }
                } else {
                    $type->native = '\\' . $typeClassName . '|' . $type->native;
                    $type->doc = $typeShortName . '|' . $type->doc;
                }
            }

            foreach ($type->use as $use) {
                $namespace->addUse($use);
            }

            $property = $class->addProperty($argument->propertyName);
            $property->setReadOnly();
            $constructorParam = $constructor->addParameter($argument->propertyName);
            $constructorParam->setType($type->native);

            if ($argument->variadic) {
                $constructor->setVariadic();
                $constructor->addComment('@param ' . $type->doc . ' ...$' . $argument->propertyName . rtrim(' ' . $argument->description));

                if ($argument->variadicMin > 0) {
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (\count(\${$argument->propertyName}) < {$argument->variadicMin}) {
                        throw new InvalidArgumentException(\sprintf('Expected at least %d values for \${$argument->propertyName}, got %d.', {$argument->variadicMin}, \count(\${$argument->propertyName})));
                    }

                    PHP);
                }

                if ($argument->variadic === VariadicType::Array) {
                    $property->setType('array');
                    $property->addComment('@var list<' . $type->doc . '> $' . $argument->propertyName . rtrim(' ' . $argument->description));
                    // Warn that named arguments are not supported
                    // @see https://psalm.dev/docs/running_psalm/issues/NamedArgumentNotAllowed/
                    $constructor->addComment('@no-named-arguments');
                    $namespace->addUseFunction('array_is_list');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (! array_is_list(\${$argument->propertyName})) {
                        throw new InvalidArgumentException('Expected \${$argument->propertyName} arguments to be a list (array), named arguments are not supported');
                    }

                    PHP);
                } elseif ($argument->variadic === VariadicType::Object) {
                    $namespace->addUse(stdClass::class);
                    $property->setType(stdClass::class);
                    $property->addComment('@var stdClass<' . $type->doc . '> $' . $argument->propertyName . rtrim(' ' . $argument->description));
                    $namespace->addUseFunction('is_string');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    foreach(\${$argument->propertyName} as \$key => \$value) {
                        if (! is_string(\$key)) {
                            throw new InvalidArgumentException('Expected \${$argument->propertyName} arguments to be a map (object), named arguments (<name>:<value>) or array unpacking ...[\'<name>\' => <value>] must be used');
                        }
                    }

                    \${$argument->propertyName} = (object) \${$argument->propertyName};
                    PHP);
                }

                $constructor->addBody('$this->' . $argument->propertyName . ' = $' . $argument->propertyName . ';');
            } else {
                // Non-variadic arguments: add validation/coercion bodies first, then property/param setup and setter
                if ($type->dollarPrefixedString) {
                    $namespace->addUseFunction('is_string');
                    $namespace->addUseFunction('str_starts_with');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (is_string(\${$argument->propertyName}) && ! str_starts_with(\${$argument->propertyName}, '$')) {
                        throw new InvalidArgumentException('Argument \${$argument->propertyName} can be an expression, field paths and variable names must be prefixed by "$" or "$$".');
                    }

                    PHP);
                }

                // List type must be validated with array_is_list()
                if ($type->list) {
                    $namespace->addUseFunction('is_array');
                    $namespace->addUseFunction('array_is_list');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (is_array(\${$argument->propertyName}) && ! array_is_list(\${$argument->propertyName})) {
                        throw new InvalidArgumentException('Expected \${$argument->propertyName} argument to be a list, got an associative array.');
                    }

                    PHP);
                }

                if ($type->query) {
                    $namespace->addUseFunction('is_array');
                    $namespace->addUse(QueryObject::class);
                    $constructor->addBody(<<<PHP
                    if (is_array(\${$argument->propertyName})) {
                        \${$argument->propertyName} = QueryObject::create(\${$argument->propertyName});
                    }

                    PHP);
                }

                if ($type->javascript) {
                    $namespace->addUseFunction('is_string');
                    $namespace->addUse(Javascript::class);
                    $constructor->addBody(<<<PHP
                    if (is_string(\${$argument->propertyName})) {
                        \${$argument->propertyName} = new Javascript(\${$argument->propertyName});
                    }

                    PHP);
                }

                if ($typeClassName !== null) {
                    $namespace->addUseFunction('is_array');
                    $propName = $argument->propertyName;
                    $constructor->addBody(<<<PHP
                    if (is_array(\${$propName})) {
                        \${$propName} = new {$typeShortName}(...\${$propName});
                    }

                    PHP);
                }

                // buildPropertyAndParam handles types/docs/defaults and adds the setter body last
                $this->buildPropertyAndParam($namespace, $property, $constructorParam, $constructor, $argument, $type);
            }
        }

        if ($encodeNames !== []) {
            $class->addConstant('PROPERTIES', $encodeNames);
        }

        return $namespace;
    }

    /**
     * Generates a dedicated type class for an operator argument with sub-fields.
     * The class is placed in MongoDB\Builder\Type namespace.
     */
    private function createArgumentTypeClass(OperatorDefinition $operator, ArgumentDefinition $argument): PhpNamespace
    {
        $typeClassName = $this->getArgumentTypeClassName($operator, $argument);
        [, $typeShortClassName] = $this->splitNamespaceAndClassName($typeClassName);

        $namespace = new PhpNamespace('MongoDB\\Builder\\Type');
        $class = $namespace->addClass($typeShortClassName);
        $class->setFinal();
        $class->addImplement(TypeInterface::class);
        $class->addComment(sprintf(
            'Type class for the $%s argument of the %s operator.',
            $argument->propertyName,
            $operator->name,
        ));
        $class->addComment('@see ' . $operator->link);

        $encodeNames = [];
        $constructor = $class->addMethod('__construct');

        foreach ($argument->arguments as $subArg) {
            $encodeNames[$subArg->propertyName] = $subArg->mergeObject ? null : $subArg->name;
            $subType = $this->getAcceptedTypes($subArg);
            foreach ($subType->use as $use) {
                $namespace->addUse($use);
            }

            $property = $class->addProperty($subArg->propertyName);
            $property->setReadOnly();
            $constructorParam = $constructor->addParameter($subArg->propertyName);

            $this->buildPropertyAndParam($namespace, $property, $constructorParam, $constructor, $subArg, $subType);
        }

        $class->addConstant('PROPERTIES', $encodeNames);

        return $namespace;
    }

    /**
     * Shared helper: sets up a property, constructor parameter, PHPDoc, default value, validation body, and property
     * setter. Handles variadic:object (map) and variadic:array (list) sub-argument patterns, as well as regular
     * non-variadic arguments.
     */
    private function buildPropertyAndParam(
        PhpNamespace $namespace,
        Property $property,
        Parameter $param,
        Method $constructor,
        ArgumentDefinition $arg,
        stdClass $type,
    ): void {
        $propName = $arg->propertyName;

        if ($arg->variadic === VariadicType::Object) {
            $namespace->addUse(stdClass::class);
            $namespace->addUseFunction('is_array');

            // Compute map VALUE doc type without Optional (optionality is at the parameter level)
            $baseArg = clone $arg;
            $baseArg->optional = false;
            $baseType = $this->getAcceptedTypes($baseArg);
            $prefix = ($arg->variadicMin ?? 0) > 0 ? 'non-empty-array' : 'array';
            $mapDocType = $prefix . '<string, ' . $baseType->doc . '>';

            if ($arg->optional) {
                $namespace->addUse(Optional::class);
                $property->setType(Optional::class . '|' . stdClass::class);
                $property->addComment('@var Optional|stdClass $' . $propName . rtrim(' ' . $arg->description));
                $param->setType(Optional::class . '|' . stdClass::class . '|array');
                $param->setDefaultValue(new Literal('Optional::Undefined'));
                $constructor->addComment('@param ' . $mapDocType . '|Optional|stdClass $' . $propName . rtrim(' ' . $arg->description));
            } else {
                $property->setType(stdClass::class);
                $property->addComment('@var stdClass<' . $baseType->doc . '> $' . $propName . rtrim(' ' . $arg->description));
                $param->setType(stdClass::class . '|array');
                $constructor->addComment('@param ' . $mapDocType . '|stdClass $' . $propName . rtrim(' ' . $arg->description));

                if (($arg->variadicMin ?? 0) > 0) {
                    $min = $arg->variadicMin;
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (\count((array) \${$propName}) < {$min}) {
                        throw new InvalidArgumentException(\sprintf('Expected at least %d entries for \${$propName}, got %d.', {$min}, \count((array) \${$propName})));
                    }

                    PHP);
                }
            }

            $constructor->addBody('$this->' . $propName . ' = is_array($' . $propName . ') ? (object) $' . $propName . ' : $' . $propName . ';');
        } elseif ($arg->variadic === VariadicType::Array) {
            $namespace->addUseFunction('is_array');
            $namespace->addUseFunction('array_is_list');
            $namespace->addUse(InvalidArgumentException::class);

            // Compute list element doc type without Optional
            $baseArg = clone $arg;
            $baseArg->optional = false;
            $baseType = $this->getAcceptedTypes($baseArg);
            $prefix = ($arg->variadicMin ?? 0) > 0 ? 'non-empty-list' : 'list';
            $listDocType = $prefix . '<' . $baseType->doc . '>';

            if ($arg->optional) {
                $namespace->addUse(Optional::class);
                $property->setType(Optional::class . '|array');
                $property->addComment('@var Optional|' . $listDocType . ' $' . $propName . rtrim(' ' . $arg->description));
                $param->setType(Optional::class . '|array');
                $param->setDefaultValue(new Literal('Optional::Undefined'));
                $constructor->addComment('@param Optional|' . $listDocType . ' $' . $propName . rtrim(' ' . $arg->description));
            } else {
                $property->setType('array');
                $property->addComment('@var ' . $listDocType . ' $' . $propName . rtrim(' ' . $arg->description));
                $param->setType('array');
                $constructor->addComment('@param ' . $listDocType . ' $' . $propName . rtrim(' ' . $arg->description));
            }

            $constructor->addBody(<<<PHP
            if (is_array(\${$propName}) && ! array_is_list(\${$propName})) {
                throw new InvalidArgumentException('Expected \${$propName} argument to be a list, got an associative array.');
            }

            PHP);
            $constructor->addBody('$this->' . $propName . ' = $' . $propName . ';');
        } else {
            // Non-variadic
            $property->setType($type->native);
            $property->addComment('@var ' . $type->doc . ' $' . $propName . rtrim(' ' . $arg->description));
            $param->setType($type->native);
            $constructor->addComment('@param ' . $type->doc . ' $' . $propName . rtrim(' ' . $arg->description));

            if ($arg->optional) {
                $namespace->addUse(Optional::class);
                $param->setDefaultValue(new Literal('Optional::Undefined'));
            } elseif ($arg->default !== null) {
                $param->setDefaultValue($arg->default);
            }

            $constructor->addBody('$this->' . $propName . ' = $' . $propName . ';');
        }
    }

    /**
     * Operator classes interfaces are defined by their return type as a MongoDB expression.
     *
     * @return list<class-string>
     */
    private function getInterfaces(OperatorDefinition $definition): array
    {
        $interfaces = [];

        foreach ($definition->type as $type) {
            $interfaces[] = $interface = $this->getType($type)->returnType;
            assert(interface_exists($interface), sprintf('"%s" is not an interface.', $interface));
        }

        return $interfaces;
    }
}
