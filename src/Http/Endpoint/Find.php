<?php

namespace MongoDB\Http\Endpoint;

use function array_key_exists;

class Find extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, $filter, array $options = [])
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'filter' => $filter,
        ];

        foreach (['projection', 'sort', 'limit', 'skip'] as $option) {
            if (array_key_exists($option, $options)) {
                $body[$option] = $options[$option];
            }
        }

        parent::__construct('data/v1/action/find', $body);
    }
}
