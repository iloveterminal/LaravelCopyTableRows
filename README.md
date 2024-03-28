# Laravel Copy Table Rows

A PHP Laravel console command to quickly copy rows from one large table to another table, and translate/convert data if necessary.

This is not a complete project, merely pieces of the core functionality, so it will require integration into an existing Laravel project to run. This project uses Laravel 5.8 and PHP 7.1.3, newer versions may require adjustments. The bulk of the core functionality can be found in 'app/Console/Commands/CopyTableRows.php'.

## Requirements

Install:

- Laravel >= v5.8
- PHP >= v7.1.3

## Execution

Run via terminal:

`php artisan database:copy-rows {sourceTable} {destinationTable} {--chunkSize=} {--startingId=} {--dataTranslation=} {--idColumn=id}`
