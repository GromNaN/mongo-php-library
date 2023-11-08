<?php

namespace MongoDB\Http\Endpoint;

class DeleteOne extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, array $filter, array $options = [])
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'filter' => $filter,
        ];

        parent::__construct('data/v1/action/deleteOne', $body);
    }
}
