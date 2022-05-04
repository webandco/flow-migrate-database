<?php

namespace Webandco\MigrateDatabase\Command;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Identifier;
use Doctrine\DBAL\Schema\Table;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Core\Booting\Scripts;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Webandco\MigrateDatabase\Persistence\Doctrine\EntityManagerFactory;

/**
 * @Flow\Scope("singleton")
 */
class MigrationCommandController extends CommandController
{
    /**
     * @var array
     * @Flow\InjectConfiguration(package="Neos.Flow");
     */
    protected $flowSettings;

    /**
     * @var array
     * @Flow\InjectConfiguration(path="structure");
     */
    protected $structureSettings;

    /**
     * @var array
     * @Flow\InjectConfiguration(path="ignoreTables");
     */
    protected $ignoreTables;

    /**
     * @var array
     * @Flow\InjectConfiguration(path="connections");
     */
    protected $connectionsConfiguration;

    /**
     * @Flow\Inject
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * Run configured Webandco.MigrateDatabase.structure.commands on the destination database
     *
     * @param string $name The destination database connection configuration used in the configuration path Webandco.MigrateDatabase.connections.[name]
     * @throws \Neos\Flow\Core\Booting\Exception\SubProcessException
     */
    public function createStructureCommand(string $name = 'destination')
    {
        if (!isset($this->connectionsConfiguration[$name])) {
            $this->output->outputLine('ERROR: Database `%s` is not configured in Webandco.MigrateDatabase.connections',
                [$name]);
            return;
        }

        putenv('WEBANDCO_MIGRATEDATABASE=' . $name);
        foreach ($this->structureSettings['commands'] as $commandName => $commandConfig) {
            $command = $commandConfig['command'];
            $arguments = $commandConfig['arguments'] ?? [];

            $this->output->outputLine('Run command (%s) %s %s',
                [$commandName, $command, \http_build_query($arguments)]);
            $start = microtime(true);
            Scripts::executeCommand($command, $this->flowSettings, true, $arguments);
            $end = microtime(true);
            $this->output->outputLine('Command %s took %.2lf s' . "\n", [$commandName, $end - $start]);
        }
    }

    /**
     * Copy rows from the source to the destination database and update sequences as needed
     *
     * @param string $from Name of the database source configuration used for Webandco.MigrateDatabase.connections.[from]
     * @param string $to Name of the database destination configuration used for Webandco.MigrateDatabase.connections.[to]
     * @param int $batch Batch size to Select and Insert if possible
     * @param bool $ignoreMissingTables If a table is missing at the destination database ignore them
     * @param bool $truncateBeforeInsert Truncate the destination tables before insert
     * @param bool $dryRun Rollbacks the transaction in the end, the insert statements are executed anyway
     * @param bool $verbose Additional console output
     * @param bool $quiet disable progressbar
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Doctrine\DBAL\Driver\Exception
     * @throws \Doctrine\DBAL\Exception
     */
    public function copyTablesCommand(
        string $from = 'source',
        string $to = 'destination',
        int $batch = 1000,
        bool $ignoreMissingTables = false,
        bool $truncateBeforeInsert = false,
        bool $dryRun = false,
        bool $verbose = false,
        bool $quiet = false
    ) {
        if (!$quiet && \version_compare(FLOW_VERSION_BRANCH, '7.1.0', '>=')) {
            // get time estimates in progress
            $this->output->getProgressBar()->setFormat('very_verbose');
        }

        $entityManagerFactory = $this->objectManager->get(EntityManagerFactory::class);

        $sourceEntityManager = $entityManagerFactory->createEntityManagerByName($from);
        $destinationEntityManager = $entityManagerFactory->createEntityManagerByName($to);

        $sourceConnection = $sourceEntityManager->getConnection();
        $destinationConnection = $destinationEntityManager->getConnection();

        !$verbose ?: $this->output->outputLine('Generating source table stats');
        $sourceTables = $this->getTableStatistics($sourceConnection);
        !$verbose ?: $this->output->outputLine('Generating destination table stats');
        $destinationTables = $this->getTableStatistics($destinationConnection);

        if ($this->ignoreTables) {
            $sourceTables = \array_diff_key($sourceTables, \array_flip($this->ignoreTables));
            $destinationTables = \array_diff_key($destinationTables, \array_flip($this->ignoreTables));
        }

        $missingDestinationTables = \array_diff_key($sourceTables, $destinationTables);
        if (!empty($missingDestinationTables)) {
            $quiet ?: $this->output->outputLine('The following tables are missing at the destination %s:',
                [$ignoreMissingTables ? 'and will be ignored' : '']);
            $quiet ?: $this->output->outputTable(\array_map(function ($value) {
                return [$value];
            }, \array_keys($missingDestinationTables)));
        }
        if ($ignoreMissingTables === false) {
            return;
        }

        if (!empty($this->ignoreTables)) {
            $tablesToCopy = \array_intersect_key($sourceTables, $destinationTables);
            foreach ($tablesToCopy as $tableName => $tableInfo) {
                if (\in_array($tableInfo['name'], $this->ignoreTables) ||
                    \in_array($tableInfo['quotedName'], $this->ignoreTables)) {
                    unset($tablesToCopy[$tableName]);
                }
            }
        }

        $allRowCount = \array_sum(\array_column($tablesToCopy, 'cnt'));;
        $quiet ?: $this->output->progressStart($allRowCount);

        $destinationConnection->beginTransaction();

        !$verbose ?: $this->output->outputLine('Disable foreign key checks');
        $this->toggleForeignKeyChecks($destinationConnection, false);

        $overAllCopied = 0;
        foreach ($tablesToCopy as $tableName => $tableInfo) {
            $offset = 0;
            $copiedRows = null;

            if ($truncateBeforeInsert) {
                !$verbose ?: $this->output->outputLine('Truncate %s', [$tableName]);
                $this->truncate($destinationConnection, $destinationTables[$tableName]['quotedName']);
            }
            !$verbose ?: $this->output->outputLine('Copy from %s', [$tableName]);

            $lastPrimaryKey = null;
            do {
                $query = 'SELECT * FROM ' . $tableInfo['quotedName'];
                switch (count($tableInfo['primaryKeyColumns'])) {
                    case 0:
                        break;
                    case 1:
                        if ($lastPrimaryKey !== null) {
                            $query .= ' WHERE ' . $tableInfo['primaryKeyColumns'][0] . ' > :primaryKey';
                        }
                    default:
                        $query .= ' ORDER BY ' . \implode(',', $tableInfo['primaryKeyColumns']);
                        break;
                }
                $query .= ' LIMIT ' . $batch;
                if (count($tableInfo['primaryKeyColumns']) !== 1 && $lastPrimaryKey === null) {
                    $query .= ' OFFSET ' . $offset;
                }

                !$verbose ?: $this->output->outputLine('%s', [$query]);
                $result = $sourceConnection->prepare($query)->executeQuery($lastPrimaryKey ? ['primaryKey' => $lastPrimaryKey] : [])->fetchAllAssociative();

                if (!empty($result)) {
                    if (count($tableInfo['primaryKeyColumns']) === 1) {
                        $last = \array_key_last($result);
                        $lastPrimaryKey = $result[$last][$tableInfo['primaryKeyColumns'][0]];
                    }

                    $colCount = count($result[0]);
                    $maxInsertRows = \floor(65000 / $colCount);
                    if (1 < $maxInsertRows) {
                        !$verbose ?: $this->output->outputLine('Max chunk size: ' . $maxInsertRows);
                        $chuncked = \array_chunk($result, $maxInsertRows);
                    } else {
                        $chuncked = [$result];
                    }

                    foreach ($chuncked as $chunk) {
                        $insert = 'INSERT INTO ' . $destinationTables[$tableName]['quotedName'];
                        $insert .= ' (' . \implode(', ', $this->quoteColumns(\array_keys($chunk[0]),
                                $destinationConnection->getDatabasePlatform())) . ')';
                        $insert .= ' VALUES ';

                        $values = [];
                        foreach ($chunk as $rowNum => $row) {
                            \array_walk_recursive(
                                $row,
                                function (&$value, $columnName) use ($destinationTables, $tableName) {
                                    if (\is_string($value) &&
                                        isset($destinationTables[$tableName]['columns'][$columnName]['type']) &&
                                        \mb_strpos($destinationTables[$tableName]['columns'][$columnName]['type'],
                                            'json') !== false &&
                                        empty($value)) {
                                        $value = '{}';
                                    }
                                }
                            );
                            if ($destinationConnection->getDatabasePlatform()->getName() === 'postgresql') {
                                // from https://stackoverflow.com/a/31672314
                                \array_walk_recursive(
                                    $row,
                                    function (&$value) {
                                        if (\is_string($value) && 0 < \mb_strlen($value)) {
                                            $value = \str_replace("\u0000", "", $value);
                                        }
                                    }
                                );
                            }

                            if (0 < $rowNum) {
                                $insert .= ',';
                            }

                            $insert .= ' (' . implode(', ', \array_fill(0, count($row), '?')) . ')';
                            $values[] = \array_values($row);
                        }

                        $destinationConnection->executeUpdate($insert, \array_merge(...$values));
                    }

                }

                $rowCount = count($result);
                $copiedRows = 0 < $rowCount;
                $offset += $rowCount;
                $quiet ?: $this->output->progressAdvance($rowCount);
            } while ($copiedRows);
        }

        $quiet ?: $this->output->progressFinish();

        $this->updateSequences($destinationConnection, $verbose);

        !$verbose ?: $this->output->outputLine('Enable foreign key checks');
        $this->toggleForeignKeyChecks($destinationConnection, true);

        if ($dryRun) {
            $destinationConnection->rollBack();
        } else {
            $destinationConnection->commit();
        }
    }

    /**
     * Return the set of tables, columns, datatypes and number of rows for each table
     *
     * @param Connection $connection
     * @return array
     * @throws \Doctrine\DBAL\Exception
     */
    protected function getTableStatistics(Connection $connection)
    {
        $tables = [];

        /** @var Table $table */
        foreach ($connection->getSchemaManager()->listTables() as $table) {
            $columns = [];
            foreach ($table->getColumns() as $column) {
                $columns[$column->getName()] = [
                    'name' => $column->getName(),
                    'quotedName' => $column->getQuotedName($connection->getDatabasePlatform()),
                    'type' => $column->getType()->getName()
                ];
            }

            $tableName = $table->getQuotedName($connection->getDatabasePlatform());
            $result = $connection->prepare('SELECT count(*) FROM ' . $tableName)->executeQuery()->fetchOne();
            $tables[$table->getName()] = [
                'name' => $table->getName(),
                'quotedName' => $tableName,
                'cnt' => $result,
                'primaryKeyColumns' => $table->getPrimaryKeyColumns(),
                'platform' => $connection->getDatabasePlatform(),
                'columns' => $columns,
            ];
        }

        return $tables;
    }

    /**
     * From https://github.com/franzose/doctrine-bulk-insert/blob/master/src/functions.php
     * Returns quoted column names
     *
     * @param array $columns
     * @param AbstractPlatform $platform
     * @return array
     */
    protected function quoteColumns(array $columns, AbstractPlatform $platform)
    {
        return \array_map(static function ($column) use ($platform) {
            return (new Identifier($column))->getQuotedName($platform);
        }, $columns);
    }

    /**
     * Enable or disable foreign key checks for mysql, postgresql or sqlite
     *
     * @param Connection $connection
     * @param bool $on
     */
    protected function toggleForeignKeyChecks(Connection $connection, bool $on)
    {
        $query = null;
        switch ($connection->getDatabasePlatform()->getName()) {
            case 'mysql':
                $query = 'SET foreign_key_checks = ' . ($on ? '1' : '0');
                break;
            case 'postgresql':
                $query = 'SET session_replication_role = \'' . ($on ? 'origin' : 'replica') . '\'';
                break;
            case 'sqlite':
                $query = 'PRAGMA foreign_keys = ' . ($on ? '1' : '0');
                break;
        }

        if ($query) {
            $connection->prepare($query)->executeQuery()->fetchOne();
        }
    }

    /**
     * Because in postgresql truncate can throw an error `cannot truncate a table referenced in a foreign key constraint`
     * we need to handle postgresql on its own
     *
     * In case of Mysql the disabled foreign key check should be enough
     *
     * @param Connection $connection
     * @param string $table
     */
    protected function truncate(Connection $connection, string $table)
    {
        $query = 'TRUNCATE TABLE ' . $table;
        switch ($connection->getDatabasePlatform()->getName()) {
            case 'postgresql':
                $query .= ' CASCADE';
                break;
        }

        if ($query) {
            $connection->prepare($query)->executeQuery()->fetchOne();
        }
    }

    /**
     * If a postgresql connection is given, all sequences are detected and updated to corresponding column max values
     *
     * @param Connection $connection
     */
    protected function updateSequences(Connection $connection, bool $verbose)
    {
        switch ($connection->getDatabasePlatform()->getName()) {
            case 'postgresql':
                $query = 'select t.schemaname as s, t.tablename as t, c.column_name as c, pg_get_serial_sequence(\'"\' || t.schemaname || \'"."\' || t.tablename || \'"\', c.column_name) as se';
                $query .= ' from pg_tables t';
                $query .= ' join information_schema.columns c on c.table_schema = t.schemaname and c.table_name = t.tablename';
                $query .= ' where t.schemaname <> \'pg_catalog\' and t.schemaname <> \'information_schema\'';
                $query .= ' and pg_get_serial_sequence(\'"\' || t.schemaname || \'"."\' || t.tablename || \'"\', c.column_name) is not null';

                $result = $connection->prepare($query)->executeQuery()->fetchAllAssociative();

                foreach ($result as $row) {
                    $query = 'SELECT setval(\'' . $row['se'] . '\', (select max(' . $row['c'] . ') from "' . $row['s'] . '"."' . $row['t'] . '"), true);';
                    $sequenceUpdate = $connection->prepare($query)->executeQuery()->fetchAllAssociative();
                    !$verbose ?: $this->output->outputLine('Sequence %s for %s.%s column %s update to %d', [
                        $row['se'],
                        $row['s'],
                        $row['t'],
                        $row['c'],
                        (int)$sequenceUpdate[0]['setval']
                    ]);
                }

                break;
        }
    }

}
