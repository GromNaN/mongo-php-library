<?php

namespace MongoDB\Tests\Operation;

use MongoDB\BSON\PackedArray;
use MongoDB\Exception\InvalidArgumentException;
use MongoDB\Operation\UpdateOne;
use TypeError;

class UpdateOneTest extends TestCase
{
    /** @dataProvider provideInvalidDocumentValues */
    public function testConstructorFilterArgumentTypeCheck($filter): void
    {
        $this->expectException($filter instanceof PackedArray ? InvalidArgumentException::class : TypeError::class);
        new UpdateOne($this->getDatabaseName(), $this->getCollectionName(), $filter, ['$set' => ['x' => 1]]);
    }

    /** @dataProvider provideInvalidDocumentValues */
    public function testConstructorUpdateArgumentTypeCheck($update): void
    {
        $this->expectException($update instanceof PackedArray ? InvalidArgumentException::class : TypeError::class);
        new UpdateOne($this->getDatabaseName(), $this->getCollectionName(), ['x' => 1], $update);
    }

    /**
     * @dataProvider provideUpdateDocuments
     * @dataProvider provideUpdatePipelines
     * @doesNotPerformAssertions
     */
    public function testConstructorUpdateArgument($update): void
    {
        new UpdateOne($this->getDatabaseName(), $this->getCollectionName(), ['x' => 1], $update);
    }

    /**
     * @dataProvider provideReplacementDocuments
     * @dataProvider provideEmptyUpdatePipelines
     */
    public function testConstructorUpdateArgumentProhibitsReplacementDocumentOrEmptyPipeline($update): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected update operator(s) or non-empty pipeline for $update');
        new UpdateOne($this->getDatabaseName(), $this->getCollectionName(), ['x' => 1], $update);
    }
}
