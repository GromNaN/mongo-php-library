<?php

namespace MongoDB\Tests\Collection;

use MongoDB\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * Functional tests for the "hideSafeContent" option, which excludes the
 * reserved __safeContent__ field written by the server on encrypted documents.
 */
class SafeContentFunctionalTest extends FunctionalTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $this->collection->insertOne([
            '_id' => 1,
            'name' => 'Bob',
            '__safeContent__' => ['reserved'],
        ]);
    }

    public function testSafeContentIsReturnedByDefault(): void
    {
        $this->assertArrayHasKey('__safeContent__', (array) $this->collection->findOne());
    }

    public function testSafeContentIsHidden(): void
    {
        $collection = $this->collection->withOptions(['hideSafeContent' => true]);
        $expected = ['_id' => 1, 'name' => 'Bob'];

        $this->assertSameDocument($expected, $collection->findOne());
        $this->assertSameDocument($expected, $collection->find()->toArray()[0]);
        $this->assertSameDocument($expected, $collection->aggregate([['$match' => ['_id' => 1]]])->toArray()[0]);
        $this->assertSameDocument($expected, $collection->findOneAndUpdate(['_id' => 1], ['$set' => ['name' => 'Rob']]));
    }

    #[DataProvider('provideProjections')]
    public function testProjection(array $expected, array $projection): void
    {
        $collection = $this->collection->withOptions(['hideSafeContent' => true]);

        $this->assertSameDocument($expected, $collection->findOne([], ['projection' => $projection]));
    }

    public static function provideProjections(): iterable
    {
        // The reserved field is excluded when the projection returns all fields
        yield 'empty' => [['_id' => 1, 'name' => 'Bob'], []];
        yield 'exclusion' => [['_id' => 1], ['name' => 0]];
        yield '_id exclusion' => [['name' => 'Bob'], ['_id' => 0]];

        // The reserved field is not returned by an inclusion projection
        yield 'inclusion' => [['_id' => 1, 'name' => 'Bob'], ['name' => 1]];
        yield '_id inclusion' => [['_id' => 1], ['_id' => 1]];

        // The projection is unchanged when it uses an operator
        yield 'slice' => [['_id' => 1, 'name' => 'Bob', '__safeContent__' => ['reserved']], ['name' => ['$slice' => 1]]];
    }

    public function testInvalidProjectionIsReported(): void
    {
        $collection = $this->collection->withOptions(['hideSafeContent' => true]);

        $this->expectException(InvalidArgumentException::class);
        $collection->find([], ['projection' => 'invalid']);
    }

    public function testAggregateDoesNotModifyPipelineEndingWithWriteStage(): void
    {
        $collection = $this->collection->withOptions(['hideSafeContent' => true]);
        $outCollection = $this->dropCollection($this->getDatabaseName(), $this->getCollectionName() . '_out');

        $collection->aggregate([
            ['$match' => ['_id' => 1]],
            ['$out' => $this->getCollectionName() . '_out'],
        ]);

        $this->assertArrayHasKey('__safeContent__', (array) $outCollection->findOne());
    }
}
