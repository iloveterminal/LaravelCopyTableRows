<?php

namespace App\Console\Commands;

use App\Helpers\DatabaseHelper;
use App\Jobs\SendMaintenanceEmail;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Quickly copy rows from one table to another table,
 * translate/convert data if necessary.
 */
class CopyRows extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'database:copy-rows {sourceTable} {destinationTable} {--chunkSize=} {--startingId=} {--dataTranslation=} {--idColumn=id}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Quickly copies rows from one table to another table.';

    /**
     * Table to copy from.
     *
     * @var string
     */
    protected $sourceTable;

    /**
     * Table to copy to.
     *
     * @var string
     */
    protected $destinationTable;

    /**
     * Mapping of values that need to be converted during copy.
     *
     * @var array
     */
    protected $translationMapping = [];

    /**
     * Columns of source table to use during translation.
     *
     * @var array
     */
    protected $sourceColumns = [];

    /**
     * Columns of destination table to use during translation.
     *
     * @var array
     */
    protected $destinationColumns = [];

    /**
     * Id column of table.
     *
     * @var string
     */
    protected $idColumn;

    /**
     * Execute the console command.
     *
     * For a non-translation execution, this command assumes the source
     * and destination table follow the same column types in the same order.
     */
    public function handle()
    {
        try {
            $this->sourceTable = $this->argument('sourceTable');
            $this->destinationTable = $this->argument('destinationTable');
            $chunkSize = $this->option('chunkSize') ?? 100000;
            $this->idColumn = $this->option('idColumn');
            $maxId = $this->getMaxIdForSourceTable();
            $dataTranslation = $this->option('dataTranslation');
            if ($dataTranslation) {
                $this->translationMapping = $this->getDataTranslationMapping($dataTranslation);
                if (!$this->translationMapping) {
                    $this->error('Aborting, translation mapping could not be found with key "' . $dataTranslation . '".');
                    exit;
                }
            }
            $this->sourceColumns = DB::getSchemaBuilder()->getColumnListing($this->sourceTable);
            $this->destinationColumns = DB::getSchemaBuilder()->getColumnListing($this->destinationTable);

            // Id of the first row to be copied, in case the command fails and needs to be resumed where it left off.
            $startingId = $this->option('startingId') ?? 1;
            $endingId = $startingId + $chunkSize;

            do {
                $this->runCopyChunk($startingId, $endingId);
                $startingId = $endingId;
                $endingId += $chunkSize;

                if ($endingId >= $maxId) {
                    /**
                     * Check if the max id on the source table has changed.
                     * This check is needed in case processes are still writing to the source table.
                     */
                    $maxId = $this->getMaxIdForSourceTable();
                }
            } while ($endingId <= $maxId);

            // Need to copy over final chunk, since query end range is not inclusive, increment it by 1.
            $endingId++;
            $this->runCopyChunk($startingId, $endingId);

            // Notify developer(s) that the copy has finished.
            $msg = "All rows have been copied from {$this->sourceTable} to {$this->destinationTable}.";
            dispatch(new SendMaintenanceEmail('Maintenance Alert', $msg));
        } catch (\Throwable $throwable) {
            Log::info($throwable);
            dispatch(new SendMaintenanceEmail('Maintenance Alert', $throwable->getMessage()));
        }
    }

    /**
     * Builds the statement for inserting rows from the source table into the destination table.
     *
     * @param int $startingId
     * @param int $endingId
     *
     * @return string
     */
    private function buildInsertStatement(int $startingId, int $endingId): string
    {
        if ($this->destinationColumns !== $this->sourceColumns) {
            $columns = implode(',', array_intersect($this->destinationColumns, $this->sourceColumns));
            $insertStatement = "INSERT INTO {$this->destinationTable} ({$columns})
                                SELECT {$columns} FROM {$this->sourceTable}
                                WHERE {$this->idColumn} >= {$startingId} and {$this->idColumn} < {$endingId}
                                ORDER BY {$this->idColumn} ASC";
        } else {
            $insertStatement = "INSERT INTO {$this->destinationTable} 
                                SELECT * FROM {$this->sourceTable}
                                WHERE {$this->idColumn} >= {$startingId} and {$this->idColumn} < {$endingId}
                                ORDER BY {$this->idColumn} ASC";
        }

        return $insertStatement;
    }

    /**
     * Returns the maximum id of the source table.
     *
     * @return int
     */
    private function getMaxIdForSourceTable(): int
    {
        return DB::table($this->sourceTable)->max($this->idColumn);
    }

    /**
     * Run the copy logic on a chunk of ids.
     *
     * @param int $startingId
     * @param int $endingId
     */
    private function runCopyChunk(int $startingId, int $endingId)
    {
        $logMessage = " copying rows with ids " . number_format($startingId) . " to " . number_format($endingId) . ".";
        Log::info("Started" . $logMessage);

        if ($this->translationMapping) {
            $this->translateCopyRows($startingId, $endingId);
        } else {
            $insertStatement = $this->buildInsertStatement($startingId, $endingId);
            DB::statement('SET FOREIGN_KEY_CHECKS=0');
            DB::statement($insertStatement);
            DB::statement('SET FOREIGN_KEY_CHECKS=1');
        }

        Log::info("Finished" . $logMessage);

        return;
    }

    /**
     * Returns the appropriate data translation mapping.
     *
     * @param string $translationKey
     *
     * @return array
     */
    private function getDataTranslationMapping(string $translationKey): array
    {
        $mapping = [];

        /*
         * Add any additional mappings following this pattern:
         * 'date_table' => [
         *      'column1' => [
         *          [
         *              'before',
         *              'after',
         *          ],
         *      ],
         *      'column2' => [
         *          [
         *              'before1',
         *              'after1',
         *          ],
         *          [
         *              'before2',
         *              'after2',
         *          ],
         *      ],
         *  ],
         */
        $mappings = [];

        if (isset($mappings[$translationKey])) {
            $mapping = $mappings[$translationKey];
        }

        return $mapping;
    }

    /**
     * Translate and copy rows from the source table into the destination table.
     *
     * @param int $startingId
     * @param int $endingId
     */
    private function translateCopyRows(int $startingId, int $endingId)
    {
        $translatedValues = [];
        $columns = array_intersect($this->destinationColumns, $this->sourceColumns);
        $columnCount = count($columns);

        $rows = DB::table($this->sourceTable)
            ->select($columns)
            ->where($this->idColumn, '>=', $startingId)
            ->where($this->idColumn, '<', $endingId)
            ->orderBy($this->idColumn, 'asc')
            ->get();

        $rows = json_decode(json_encode($rows), true);
        foreach ($rows as $row) {
            $rowValues = [];
            foreach ($row as $column => $value) {
                if (isset($this->translationMapping[$column])) {
                    // Convert column values if necessary.
                    foreach ($this->translationMapping[$column] as $translation) {
                        if ($value === $translation[0]) {
                            $value = $translation[1];
                        }
                    }
                }
                $rowValues[] = $value;
            }
            if (count($rowValues) !== $columnCount) {
                // Column count and value count must match for bulk insert to work.
                $exception = 'Aborting, row values count does not match column count: ' . implode(',', $rowValues);
                throw new \Exception($exception);
            }
            $translatedValues[] = $rowValues;
        }

        if ($translatedValues) {
            // Need to chunk inserts to avoid errors with too many placeholders.
            foreach (array_chunk($translatedValues, 5000) as $chunk) {
                // The default ->insert() call is too slow, use custom bulkInsert instead.
                DB::statement('SET FOREIGN_KEY_CHECKS=0');
                DatabaseHelper::bulkInsert($this->destinationTable, $columns, $chunk);
                DB::statement('SET FOREIGN_KEY_CHECKS=1');
            }
        }

        return;
    }
}