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

import com.facebook.presto.raptorx.TransactionManager;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.ShardHashing.tableShard;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class CommitCleaner
{
    private static final int CHUNK_BATCH_SIZE = 1000;

    private final TransactionManager transactionManager;
    private final MasterCommitCleanerDao masterDao;
    private final DeletedChunksDao deletedChunksDao;
    private final List<Jdbi> shardDbi;

    @Inject
    public CommitCleaner(TransactionManager transactionManager, Database database)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        this.masterDao = createJdbi(database.getMasterConnection())
                .onDemand(MasterCommitCleanerDao.class);

        this.deletedChunksDao = createDeletedChunksDao(database);

        this.shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
    }

    public synchronized void removeOldCommits()
    {
        long activeCommitId = transactionManager.oldestActiveCommitId();

        Set<Long> droppedTableIds = masterDao.getDroppedTableIds(activeCommitId);

        Map<Integer, Set<Long>> droppedTableIdsByShard = droppedTableIds.stream()
                .collect(groupingBy(tableId -> tableShard(tableId, shardDbi.size()), toSet()));

        // drop index tables
        for (long tableId : droppedTableIds) {
            try (Handle handle = shardDbi.get(tableShard(tableId, shardDbi.size())).open()) {
                handle.execute("DROP TABLE IF EXISTS " + chunkIndexTable(tableId));
            }
        }

        // cleanup deleted chunks
        long now = System.currentTimeMillis();
        for (int i = 0; i < shardDbi.size(); i++) {
            ShardCommitCleanerDao shard = shardDbi.get(i).onDemand(ShardCommitCleanerDao.class);
            Set<Long> tableIds = droppedTableIdsByShard.getOrDefault(i, ImmutableSet.of());

            // cleanup chunks for old commits
            List<TableChunk> chunks;
            do {
                chunks = shard.getDeletedChunks(activeCommitId, CHUNK_BATCH_SIZE);
                if (!chunks.isEmpty()) {
                    cleanupChunks(shard, now, chunks, tableIds);
                }
            }
            while (chunks.size() == CHUNK_BATCH_SIZE);

            // cleanup chunks for dropped tables
            if (!tableIds.isEmpty()) {
                do {
                    chunks = shard.getDeletedChunks(tableIds, CHUNK_BATCH_SIZE);
                    if (!chunks.isEmpty()) {
                        cleanupChunks(shard, now, chunks, tableIds);
                    }
                }
                while (chunks.size() == CHUNK_BATCH_SIZE);
            }
        }

        // cleanup dropped table columns
        if (!droppedTableIds.isEmpty()) {
            masterDao.cleanupDroppedTableColumns(droppedTableIds);
        }

        // cleanup schemas, tables, columns, views, commits
        masterDao.cleanup(activeCommitId);

        // cleanup table sizes
        for (Jdbi shard : shardDbi) {
            shard.onDemand(ShardCommitCleanerDao.class)
                    .cleanupTableSizes(activeCommitId);
        }
    }

    private void cleanupChunks(ShardCommitCleanerDao shard, long now, List<TableChunk> chunks, Set<Long> droppedTableIds)
    {
        // queue for deletion
        deletedChunksDao.insertDeletedChunks(now, chunks);

        // delete from index table
        chunks.stream()
                .filter(chunk -> !droppedTableIds.contains(chunk.getTableId()))
                .collect(groupingBy(TableChunk::getTableId))
                .forEach((tableId, tableChunks) ->
                        shard.deleteIndexChunks(chunkIndexTable(tableId), toChunkIds(tableChunks)));

        // cleanup deleted chunks
        shard.deleteChunks(toChunkIds(chunks));
    }

    private static Set<Long> toChunkIds(Collection<TableChunk> tableChunks)
    {
        return tableChunks.stream()
                .map(TableChunk::getChunkId)
                .collect(toImmutableSet());
    }

    private static DeletedChunksDao createDeletedChunksDao(Database database)
    {
        Jdbi dbi = createJdbi(database.getMasterConnection());
        switch (database.getType()) {
            case H2:
            case MYSQL:
                return dbi.onDemand(MySqlDeletedChunksDao.class);
            case POSTGRESQL:
                return dbi.onDemand(PostgreSqlDeletedChunksDao.class);
        }
        throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled database: " + database.getType());
    }

    public interface DeletedChunksDao
    {
        void insertDeletedChunks(long deleteTime, Iterable<TableChunk> chunks);
    }

    public interface MySqlDeletedChunksDao
            extends DeletedChunksDao
    {
        @Override
        @SqlBatch("INSERT IGNORE INTO deleted_chunks (chunk_id, size, delete_time, purge_time)\n" +
                "VALUES (:chunkId, :size, :deleteTime, :deleteTime)")
        void insertDeletedChunks(
                @Bind long deleteTime,
                @BindBean Iterable<TableChunk> chunks);
    }

    public interface PostgreSqlDeletedChunksDao
            extends DeletedChunksDao
    {
        @Override
        @SqlBatch("INSERT INTO deleted_chunks (chunk_id, size, delete_time, purge_time)\n" +
                "VALUES (:chunkId, :size, :deleteTime, :deleteTime)\n" +
                "ON CONFLICT DO NOTHING\n")
        void insertDeletedChunks(
                @Bind long deleteTime,
                @BindBean Iterable<TableChunk> chunks);
    }
}
