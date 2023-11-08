<?php

namespace MongoDB\Http\Endpoint;

use function array_key_exists;

class UpdateMany extends Endpoint
{
    public function __construct(string $databaseName, string $collectionName, array $filter, array $update, array $options = [])
    {
        $body = [
            'dataSource' => 'mongodb-atlas',
            'database' => $databaseName,
            'collection' => $collectionName,
            'filter' => $filter,
            'update' => $update,
        ];

        foreach (['upsert'] as $option) {
            if (array_key_exists($option, $options)) {
                $body[$option] = $options[$option];
            }
        }

        parent::__construct('data/v1/action/updateMany', $body);
    }
}
