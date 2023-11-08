<?php

namespace MongoDB\Http\Endpoint;

use function array_key_exists;

class FindOne extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, array $filter, array $options = [])
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'filter' => $filter,
        ];

        foreach (['projection'] as $option) {
            if (array_key_exists($option, $options)) {
                $body[$option] = $options[$option];
            }
        }

        parent::__construct('data/v1/action/findOne', $body);
    }
}
