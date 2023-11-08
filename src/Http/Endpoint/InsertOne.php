<?php

namespace MongoDB\Http\Endpoint;

class InsertOne extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, array $document)
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'document' => $document,
        ];

        parent::__construct('data/v1/action/insertOne', $body);
    }
}
