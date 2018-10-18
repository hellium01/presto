/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.metadata.RollbackInfo.TableColumn;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.ColumnStats;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.metadata.IndexWriter.addIndexTableColumn;
import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.createIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.dropIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.dropIndexTableColumn;
import static com.facebook.presto.raptorx.metadata.ShardHashing.tableShard;
import static com.facebook.presto.raptorx.util.DatabaseUtil.boxedInt;
import static com.facebook.presto.raptorx.util.DatabaseUtil.boxedLong;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.raptorx.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8Bytes;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * The single writer of all transactional metadata.
 * <p>
 * The transaction system maintains the invariant that commits are written
 * non-concurrently in sequential order. This ensures that a view of the
 * database as of a given commit is immutable. Readers do not need to filter
 * out data for in-progress or aborted transactions. If a commit fails, it
 * must be fully rolled back before the next commit can start.
 * <p>
 * The first step of each master protocol action is to acquire a lock on the
 * {@code current_commit} row. This serves as a global lock within the master
 * database and thus guarantees no intervening database writes are possible.
 * <p>
 * The shard databases cannot rely on the master lock because it could be lost
 * while a shard write is in progress. Instead, they use a barrier which both
 * serves as a lock and prevents errant writes for aborted commits.
 * The {@code aborted_commit} row indicates the most recent aborted commit and
 * only allows writing data for newer commits. During normal operation without
 * rollbacks, the barrier does not need to be updated. When a rollback occurs,
 * it is necessary to update the barrier on every shard.
 * <p>
 * Commit protocol:
 * <ul>
 * <li>Starting a commit:</li>
 * <ul>
 * <li>Verify that {@code active_commit} row does not exist</li>
 * <li>Allocate {@code commitId} and verify it is larger than the current commit</li>
 * <li>Insert {@code active_commit} row</li>
 * </ul>
 * <li>Writing to the master:</li>
 * <ul>
 * <li>Verify {@code active_commit} row has correct {@code commitId} and is not rolling back</li>
 * <li>Perform write operation</li>
 * </ul>
 * <li>Writing to a shard:</li>
 * <ul>
 * <li>Lock {@code aborted_commit} row and verify it is smaller than the current commit</li>
 * <li>Perform write operation</li>
 * </ul>
 * <li>Finishing a commit:</li>
 * <ul>
 * <li>Verify {@code active_commit} row</li>
 * <li>Delete {@code active_commit} row</li>
 * <li>Insert {@code commits} row</li>
 * <li>Update {@code current_commit} row</li>
 * </ul>
 * </ul>
 * Rollback protocol:
 * <ul>
 * <li>Starting a rollback:</li>
 * <ul>
 * <li>Mark {@code active_commit} row as aborted</li>
 * </ul>
 * <li>Writing to the master:</li>
 * <ul>
 * <li>Verify {@code active_commit} row has correct {@code commitId}</li>
 * <li>Perform write operation</li>
 * </ul>
 * <li>Writing to a shard:</li>
 * <ul>
 * <li>Lock {@code aborted_commit} row and verify it is no larger than the current commit</li>
 * <li>Update {@code aborted_commit} row</li>
 * <li>Perform write operation</li>
 * </ul>
 * <li>Finishing a rollback:</li>
 * <ul>
 * <li>Verify {@code active_commit} row</li>
 * <li>Delete {@code active_commit} row</li>
 * </ul>
 * </ul>
 */
public class DatabaseMetadataWriter
        implements MetadataWriter
{
    private final SequenceManager sequenceManager;
    private final Database.Type databaseType;
    private final Jdbi masterDbi;
    private final List<Jdbi> shardDbi;

    @Inject
    public DatabaseMetadataWriter(SequenceManager sequenceManager, Database database, TypeManager typeManager)
    {
        this.sequenceManager = requireNonNull(sequenceManager, "sequenceManager is null");
        this.databaseType = requireNonNull(database.getType(), "database.type is null");

        Jdbi masterDbi = createJdbi(database.getMasterConnection());
        masterDbi.registerRowMapper(new ColumnInfo.Mapper(typeManager));
        this.masterDbi = masterDbi;

        this.shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
    }

    @Override
    public void recover()
    {
        // start rollback
        ActiveCommit activeCommit = masterDbi.inTransaction(handle -> {
            MasterWriterDao dao = handle.attach(MasterWriterDao.class);

            // acquire global lock
            getLockedCurrentCommitId(dao);

            // abort current commit
            ActiveCommit commit = dao.getActiveCommit();
            if (commit == null) {
                return null;
            }
            if (!commit.isRollingBack()) {
                dao.abortActiveCommit();
            }

            // rollback master
            dao.rollback(commit.getCommitId());

            return commit;
        });

        // skip if no rollback needed
        if (activeCommit == null) {
            return;
        }

        long commitId = activeCommit.getCommitId();
        RollbackInfo rollbackInfo = activeCommit.getRollbackInfo();

        // rollback shards
        for (Jdbi shardDbi : shardDbi) {
            shardDbi.useTransaction(handle -> {
                ShardWriterDao dao = handle.attach(ShardWriterDao.class);

                // acquire shard lock
                long abortedCommitId = getLockedAbortedCommitId(dao);

                // skip if no rollback needed
                if (abortedCommitId > commitId) {
                    return;
                }

                dao.updateAbortedCommitId(commitId);

                // rollback shard
                dao.rollback(commitId);

                for (long tableId : rollbackInfo.getWrittenTableIds()) {
                    dao.rollbackCreatedIndexChunks(chunkIndexTable(tableId), commitId);
                    dao.rollbackDeletedIndexChunks(chunkIndexTable(tableId), commitId);
                }
            });
        }

        // rollback indexes
        for (long tableId : rollbackInfo.getCreatedTableIds()) {
            shardDbi.get(getTableShard(tableId)).useHandle(handle ->
                    dropIndexTable(handle, tableId));
        }

        for (TableColumn column : rollbackInfo.getAddedColumns()) {
            shardDbi.get(getTableShard(column.getTableId())).useHandle(handle ->
                    dropIndexTableColumn(databaseType, handle, column.getTableId(), column.getColumnId()));
        }

        // finish rollback
        masterDbi.useTransaction(handle -> {
            MasterWriterDao dao = handle.attach(MasterWriterDao.class);

            // acquire global lock
            getLockedCurrentCommitId(dao);

            // verify we are still rolling back
            ActiveCommit commit = dao.getActiveCommit();
            if ((commit == null) || (commit.getCommitId() != commitId)) {
                return;
            }
            verifyMetadata(commit.isRollingBack(), "Commit should be rolling back");

            // finalize rollback
            deleteActiveCommit(dao);
        });
    }

    @Override
    public long beginCommit()
    {
        return masterDbi.inTransaction(handle -> {
            MasterWriterDao dao = handle.attach(MasterWriterDao.class);

            // acquire global lock
            long currentCommitId = getLockedCurrentCommitId(dao);

            // verify no commit in progress
            if (dao.getActiveCommit() != null) {
                throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Commit already in progress");
            }

            // allocate commit ID
            long commitId = sequenceManager.nextValue("commit_id", 1);
            verifyMetadata(commitId > currentCommitId, "Invalid next commit ID: %s", commitId);

            // start commit
            dao.insertActiveCommit(commitId, System.currentTimeMillis());

            return commitId;
        });
    }

    @Override
    public void finishCommit(long commitId, OptionalLong transactionId)
    {
        writeMaster(commitId, (dao, commit) -> {
            deleteActiveCommit(dao);

            dao.insertCommit(commitId, commit.getStartTime());

            verifyMetadata(dao.updateCurrentCommit(commitId) == 1, "Wrong row count for current_commit update");

            if (transactionId.isPresent()) {
                checkConflict(dao.finalizeTransaction(transactionId.getAsLong()) == 1, "Transaction was aborted");
            }
        });
    }

    @Override
    public void createSchema(long commitId, long schemaId, String schemaName)
    {
        writeMaster(commitId, (dao, commit) -> {
            byte[] name = schemaName.getBytes(UTF_8);
            checkConflict(!dao.schemaNameExists(name), "Schema already exists: %s", schemaName);
            dao.insertSchema(commitId, schemaId, name);
        });
    }

    @Override
    public void renameSchema(long commitId, long schemaId, String newSchemaName)
    {
        writeMaster(commitId, (dao, commit) -> {
            byte[] newName = newSchemaName.getBytes(UTF_8);
            checkConflict(dao.schemaIdExists(schemaId), "Schema no longer exists");
            checkConflict(!dao.schemaNameExists(newName), "Schema already exists: %s", newSchemaName);

            deleteSchema(dao, commitId, schemaId);
            dao.insertSchema(commitId, schemaId, newName);
        });
    }

    @Override
    public void dropSchema(long commitId, long schemaId)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.schemaIdExists(schemaId), "Schema no longer exists");
            deleteSchema(dao, commitId, schemaId);
        });
    }

    @Override
    public void createTable(long commitId, TableInfo table)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.distributionIdExists(table.getDistributionId()), "Distribution no longer exists");
            failIfRelationExists(dao, table.getSchemaId(), table.getTableName());

            insertTable(dao, commitId, table);

            for (ColumnInfo column : table.getColumns()) {
                insertColumn(dao, commitId, table.getTableId(), column);
            }

            dao.updateRollbackInfo(commit.getRollbackInfo()
                    .withCreatedTable(table.getTableId())
                    .toBytes());
        });

        writeShard(commitId, getTableShard(table.getTableId()), dao -> {
            createIndexTable(
                    databaseType,
                    dao.getHandle(),
                    table.getTableId(),
                    table.getColumns(),
                    table.getTemporalColumnId());

            dao.insertTableSize(commitId, table.getTableId(), 0, 0, 0);
        });
    }

    @Override
    public void renameTable(long commitId, long tableId, long schemaId, String tableName)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");
            failIfRelationExists(dao, schemaId, tableName);

            TableInfo table = dao.getTableInfo(tableId).builder()
                    .setTableName(tableName)
                    .setSchemaId(schemaId)
                    .setUpdateTime(System.currentTimeMillis())
                    .build();

            deleteTable(dao, commitId, tableId);
            insertTable(dao, commitId, table);
        });
    }

    @Override
    public void dropTable(long commitId, long tableId)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");
            deleteTable(dao, commitId, tableId);
        });

        writeShard(commitId, getTableShard(tableId), dao ->
                dao.deleteTableSize(commitId, tableId));
    }

    @Override
    public void addColumn(long commitId, long tableId, ColumnInfo column)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");

            List<ColumnInfo> columns = dao.getColumnInfo(tableId);

            boolean nameExists = columns.stream().anyMatch(info -> info.getColumnName().equals(column.getColumnName()));
            checkConflict(!nameExists, "Column already exists: %s", column.getColumnName());

            boolean ordinalExists = columns.stream().anyMatch(info -> info.getColumnId() == column.getOrdinal());
            checkConflict(!ordinalExists, "Column ordinal already exists");

            insertColumn(dao, commitId, tableId, column);

            updateTable(dao, commitId, tableId);

            dao.updateRollbackInfo(commit.getRollbackInfo()
                    .withAddedColumn(tableId, column.getColumnId())
                    .toBytes());
        });

        writeShard(commitId, getTableShard(tableId), dao ->
                addIndexTableColumn(databaseType, dao.getHandle(), tableId, column));
    }

    @Override
    public void renameColumn(long commitId, long tableId, long columnId, String columnName)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");

            List<ColumnInfo> columns = dao.getColumnInfo(tableId);
            ColumnInfo column = columns.stream()
                    .filter(info -> info.getColumnId() == columnId)
                    .findAny()
                    .orElseThrow(() -> new PrestoException(TRANSACTION_CONFLICT, "Column no longer exists"));

            boolean exists = columns.stream().anyMatch(info -> info.getColumnName().equals(columnName));
            checkConflict(!exists, "Column already exists: %s", columnName);

            deleteColumn(dao, commitId, tableId, columnId);
            insertColumn(dao, commitId, tableId, column.withColumnName(columnName));

            updateTable(dao, commitId, tableId);
        });
    }

    @Override
    public void dropColumn(long commitId, long tableId, long columnId)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");

            List<ColumnInfo> columns = dao.getColumnInfo(tableId);
            checkConflict(columns.stream().anyMatch(column -> column.getColumnId() == columnId), "Column no longer exists");
            checkConflict(columns.size() > 1, "Table only has one column");

            deleteColumn(dao, commitId, tableId, columnId);

            updateTable(dao, commitId, tableId);
        });
    }

    @Override
    public void createView(long commitId, ViewInfo view)
    {
        writeMaster(commitId, (dao, commit) -> {
            failIfRelationExists(dao, view.getSchemaId(), view.getViewName());
            dao.insertView(
                    commitId,
                    view.getViewId(),
                    view.getViewName().getBytes(UTF_8),
                    view.getSchemaId(),
                    view.getCreateTime(),
                    view.getUpdateTime(),
                    view.getViewData().getBytes(UTF_8),
                    utf8Bytes(view.getComment()));
        });
    }

    @Override
    public void dropView(long commitId, long viewId)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.viewIdExists(viewId), "View no longer exists");
            deleteView(dao, commitId, viewId);
        });
    }

    @Override
    public void insertChunks(long commitId, long tableId, Collection<ChunkInfo> chunks)
    {
        Set<Long> columnIds = chunks.stream()
                .map(ChunkInfo::getColumnStats)
                .flatMap(Collection::stream)
                .map(ColumnStats::getColumnId)
                .collect(toImmutableSet());

        long currentTime = System.currentTimeMillis();
        AtomicReference<List<ColumnInfo>> columns = new AtomicReference<>();
        AtomicReference<TableInfo> table = new AtomicReference<>();

        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");

            table.set(dao.getTableInfo(tableId).builder()
                    .addRowCount(chunks.stream().mapToLong(ChunkInfo::getRowCount).sum())
                    .setUpdateTime(currentTime)
                    .build());

            columns.set(dao.getColumnInfo(tableId));
            Set<Long> tableColumnIds = columns.get().stream()
                    .map(ColumnInfo::getColumnId)
                    .collect(toSet());
            checkConflict(tableColumnIds.containsAll(columnIds), "Column no longer exists");

            deleteTable(dao, commitId, tableId);
            insertTable(dao, commitId, table.get());

            dao.updateRollbackInfo(commit.getRollbackInfo()
                    .withWrittenTable(tableId)
                    .toBytes());
        });

        writeShard(commitId, getTableShard(tableId), dao -> {
            dao.insertChunks(commitId, tableId, currentTime, chunks.stream()
                    .map(chunk -> ChunkMetadata.from(table.get(), chunk))
                    .iterator());

            try (IndexWriter writer = new IndexWriter(dao.getHandle().getConnection(), commitId, tableId, columns.get())) {
                for (ChunkInfo chunk : chunks) {
                    writer.add(chunk.getChunkId(), chunk.getBucketNumber(), chunk.getColumnStats());
                }
                writer.execute();
            }
            catch (SQLException e) {
                throw metadataError(e);
            }

            TableSize tableSize = dao.getTableSize(tableId);
            long chunkCount = tableSize.getChunkCount();
            long compressedSize = tableSize.getCompressedSize();
            long uncompressedSize = tableSize.getUncompressedSize();

            chunkCount += chunks.size();
            compressedSize += chunks.stream().mapToLong(ChunkInfo::getCompressedSize).sum();
            uncompressedSize += chunks.stream().mapToLong(ChunkInfo::getUncompressedSize).sum();

            dao.deleteTableSize(commitId, tableId);
            dao.insertTableSize(commitId, tableId, chunkCount, compressedSize, uncompressedSize);
        });
    }

    @Override
    public void deleteChunks(long commitId, long tableId, Set<Long> chunkIds)
    {
        writeMaster(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");
            dao.updateRollbackInfo(commit.getRollbackInfo()
                    .withWrittenTable(tableId)
                    .toBytes());
        });

        AtomicLong rowCount = new AtomicLong();

        writeShard(commitId, getTableShard(tableId), dao -> {
            ChunkSummary summary = dao.getChunkSummary(commitId, chunkIds);
            checkTableConflict(summary.getChunkCount() == chunkIds.size());
            rowCount.set(summary.getRowCount());

            deleteChunks(dao, commitId, tableId, chunkIds);

            TableSize tableSize = dao.getTableSize(tableId);
            long chunkCount = tableSize.getChunkCount() - summary.getChunkCount();
            long compressedSize = tableSize.getCompressedSize() - summary.getCompressedSize();
            long uncompressedSize = tableSize.getUncompressedSize() - summary.getUncompressedSize();

            dao.deleteTableSize(commitId, tableId);
            dao.insertTableSize(commitId, tableId, chunkCount, compressedSize, uncompressedSize);
        });

        writeMaster(commitId, (dao, commit) -> {
            TableInfo table = dao.getTableInfo(tableId).builder()
                    .addRowCount(-rowCount.get())
                    .setUpdateTime(System.currentTimeMillis())
                    .build();

            deleteTable(dao, commitId, tableId);
            insertTable(dao, commitId, table);
        });
    }

    private void writeMaster(long commitId, BiConsumer<MasterWriterDao, ActiveCommit> writer)
    {
        masterDbi.useTransaction(handle -> {
            MasterWriterDao dao = handle.attach(MasterWriterDao.class);
            ActiveCommit commit = getLockedActiveCommit(dao, commitId);
            writer.accept(dao, commit);
        });
    }

    private void writeShard(long commitId, int shard, Consumer<ShardWriterDao> writer)
    {
        shardDbi.get(shard).useTransaction(handle -> {
            ShardWriterDao dao = handle.attach(ShardWriterDao.class);
            long abortedCommitId = getLockedAbortedCommitId(dao);
            if (abortedCommitId >= commitId) {
                throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Commit is no longer active: " + commitId);
            }
            writer.accept(dao);
        });
    }

    private int getTableShard(long tableId)
    {
        return tableShard(tableId, shardDbi.size());
    }

    private static void failIfRelationExists(MasterWriterDao dao, long schemaId, String name)
    {
        checkConflict(!dao.tableNameExists(schemaId, name.getBytes(UTF_8)), "Table already exists: %s", name);
        checkConflict(!dao.viewNameExists(schemaId, name.getBytes(UTF_8)), "View already exists: %s", name);
    }

    private static void insertTable(MasterWriterDao dao, long commitId, TableInfo table)
    {
        dao.insertTable(
                commitId,
                table.getTableId(),
                table.getTableName().getBytes(UTF_8),
                table.getSchemaId(),
                table.getDistributionId(),
                boxedLong(table.getTemporalColumnId()),
                table.getCreateTime(),
                table.getUpdateTime(),
                table.getRowCount(),
                utf8Bytes(table.getComment()));
    }

    private static void insertColumn(MasterWriterDao dao, long commitId, long tableId, ColumnInfo column)
    {
        dao.insertColumn(
                commitId,
                column.getColumnId(),
                column.getColumnName().getBytes(UTF_8),
                column.getType().getTypeSignature().toString().getBytes(UTF_8),
                tableId,
                column.getOrdinal(),
                boxedInt(column.getBucketOrdinal()),
                boxedInt(column.getSortOrdinal()),
                utf8Bytes(column.getComment()));
    }

    private static void updateTable(MasterWriterDao dao, long commitId, long tableId)
    {
        TableInfo table = dao.getTableInfo(tableId).builder()
                .setUpdateTime(System.currentTimeMillis())
                .build();

        deleteTable(dao, commitId, tableId);
        insertTable(dao, commitId, table);
    }

    private static void deleteSchema(MasterWriterDao dao, long commitId, long schemaId)
    {
        int rows = dao.deleteSchema(commitId, schemaId);
        verifyMetadata(rows == 1, "Wrong row count %s for delete of schema (%s)", rows, schemaId);
    }

    private static void deleteTable(MasterWriterDao dao, long commitId, long tableId)
    {
        int rows = dao.deleteTable(commitId, tableId);
        verifyMetadata(rows == 1, "Wrong row count %s for delete of table (%s)", rows, tableId);
    }

    private static void deleteView(MasterWriterDao dao, long commitId, long viewId)
    {
        int rows = dao.deleteView(commitId, viewId);
        verifyMetadata(rows == 1, "Wrong row count %s for delete of view (%s)", rows, viewId);
    }

    private static void deleteColumn(MasterWriterDao dao, long commitId, long tableId, long columnId)
    {
        int rows = dao.deleteColumn(commitId, tableId, columnId);
        verifyMetadata(rows == 1, "Wrong row count %s for delete of column (%s:%s)", rows, tableId, columnId);
    }

    private static void deleteChunks(ShardWriterDao dao, long commitId, long tableId, Set<Long> chunkIds)
    {
        int rows = dao.deleteChunks(commitId, chunkIds);
        verifyMetadata(rows == chunkIds.size(), "Wrong row count %s for delete of %s chunks", rows, chunkIds.size());

        rows = dao.deleteIndexChunks(chunkIndexTable(tableId), chunkIds);
        verifyMetadata(rows == chunkIds.size(), "Wrong row count %s for delete of %s index chunks", rows, chunkIds.size());
    }

    private static void deleteActiveCommit(MasterWriterDao dao)
    {
        int rows = dao.deleteActiveCommit();
        verifyMetadata(rows == 1, "Wrong row count %s for active_commit delete", rows);
    }

    private static ActiveCommit getLockedActiveCommit(MasterWriterDao dao, long commitId)
    {
        // acquire global lock
        long currentCommitId = getLockedCurrentCommitId(dao);

        // verify that we are still committing
        ActiveCommit commit = dao.getActiveCommit();
        if ((commit == null) || (commit.getCommitId() != commitId) || commit.isRollingBack()) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Commit is no longer active: " + commitId);
        }

        // verify database state
        verifyMetadata(currentCommitId < commitId, "Commit is older than current_commit row");

        return commit;
    }

    private static long getLockedCurrentCommitId(MasterWriterDao dao)
    {
        Long id = dao.getLockedCurrentCommitId();
        verifyMetadata(id != null, "No current_commit row");
        return id;
    }

    private static long getLockedAbortedCommitId(ShardWriterDao dao)
    {
        Long id = dao.getLockedAbortedCommitId();
        verifyMetadata(id != null, "No aborted_commit row");
        return id;
    }

    private static void checkConflict(boolean condition, String format, Object... args)
    {
        if (!condition) {
            throw new PrestoException(TRANSACTION_CONFLICT, format(format, args));
        }
    }

    private static void checkTableConflict(boolean condition)
    {
        checkConflict(condition, "Table was updated by a different transaction. Please retry the operation.");
    }
}
