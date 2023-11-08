<?php

namespace MongoDB\Http\Endpoint;

class InsertMany extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, array $documents)
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'documents' => $documents,
        ];

        parent::__construct('data/v1/action/insertMany', $body);
    }
}
