<?php

namespace MongoDB\Http\Endpoint;

class Aggregate extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, array $pipeline, array $options)
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'pipeline' => $pipeline,
        ];

        parent::__construct('data/v1/action/aggregate', $body);
    }
}
