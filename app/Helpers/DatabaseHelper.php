<?php

namespace App\Helpers;

use Illuminate\Support\Facades\DB;

/**
 * Helper methods for database.
 */
class DatabaseHelper
{
    /**
     * Run a fast bulk insert query, avoids overhead of default '->insert()' method.
     *
     * @param string $tableName
     * @param array  $columns
     * @param array  $values
     *
     * @return bool
     */
    public static function bulkInsert(string $tableName, array $columns, array $values): bool
    {
        $columnsString = implode(
            ',',
            array_map(function ($value) {
                return "`{$value}`";
            }, $columns)
        );

        $valuesString = implode(
            ',',
            array_map(function ($row) {
                return '(' . implode(
                    ',',
                    array_map(function ($value) {
                        if ($value === null) {
                            return "NULL";
                        }

                        return DB::connection()->getPdo()->quote($value);
                    }, $row)
                ) . ')';
            }, $values)
        );

        $sql = "INSERT INTO {$tableName} ({$columnsString}) VALUES {$valuesString}";

        return DB::statement($sql);
    }
}