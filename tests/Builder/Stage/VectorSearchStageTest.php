<?php

declare(strict_types=1);

namespace MongoDB\Tests\Builder\Stage;

use MongoDB\Builder\Pipeline;
use MongoDB\Builder\Query;
use MongoDB\Builder\Stage;
use MongoDB\Tests\Builder\PipelineTestCase;

/**
 * Test $vectorSearch stage
 */
class VectorSearchStageTest extends PipelineTestCase
{
    public function testANNBasic(): void
    {
        $pipeline = new Pipeline(
            Stage::vectorSearch(
                index: 'vector_index',
                path: 'plot_embedding',
                queryVector: [-0.0016261312, -0.028070757, -0.011342932],
                numCandidates: 150,
                limit: 10,
            ),
            Stage::project(
                _id: 0,
                plot: 1,
                title: 1,
                score: ['$meta' => 'vectorSearchScore'],
            ),
        );

        $this->assertSamePipeline(Pipelines::VectorSearchANNBasic, $pipeline);
    }

    public function testANNFilter(): void
    {
        $pipeline = new Pipeline(
            Stage::vectorSearch(
                index: 'vector_index',
                limit: 10,
                path: 'plot_embedding',
                queryVector: [0.02421053, -0.022372592, -0.006231137],
                filter: Query::and(
                    Query::query(
                        year: Query::lt(1975),
                    ),
                ),
                numCandidates: 150,
            ),
            Stage::project(
                _id: 0,
                title: 1,
                plot: 1,
                year: 1,
                score: ['$meta' => 'vectorSearchScore'],
            ),
        );

        $this->assertSamePipeline(Pipelines::VectorSearchANNFilter, $pipeline);
    }

    public function testENN(): void
    {
        $pipeline = new Pipeline(
            Stage::vectorSearch(
                index: 'vector_index',
                limit: 10,
                path: 'plot_embedding',
                queryVector: [-0.006954097, -0.009932499, -0.001311474],
                exact: true,
            ),
            Stage::project(
                _id: 0,
                title: 1,
                plot: 1,
                score: ['$meta' => 'vectorSearchScore'],
            ),
        );

        $this->assertSamePipeline(Pipelines::VectorSearchENN, $pipeline);
    }

    public function testStoredSource(): void
    {
        $pipeline = new Pipeline(
            Stage::vectorSearch(
                index: 'vector_index',
                path: 'plot_embedding',
                queryVector: [-0.03994801267981529, -0.016522614285349846, -0.008775344118475914],
                filter: Query::and(
                    Query::query(
                        year: Query::gt(1970),
                    ),
                    Query::query(
                        year: Query::lt(2020),
                    ),
                    Query::query(
                        genres: Query::in(['Action', 'Drama', 'Comedy']),
                    ),
                ),
                limit: 10,
                numCandidates: 1000,
                returnStoredSource: true,
            ),
            Stage::project(
                _id: 0,
                plot: 1,
                title: 1,
                genres: 1,
                score: ['$meta' => 'vectorSearchScore'],
            ),
        );

        $this->assertSamePipeline(Pipelines::VectorSearchStoredSource, $pipeline);
    }
}
