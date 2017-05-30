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
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.SchemaTableName;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalLong;

import static com.facebook.presto.raptorx.metadata.TestingSchema.createTestingSchema;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestCommitCleaner
{
    @Test
    public void testRemoveOldCommits()
    {
        Database database = new TestingDatabase();
        new SchemaCreator(database).create();

        TestingEnvironment environment = new TestingEnvironment(database);
        Metadata metadata = environment.getMetadata();

        TransactionManager transactionManager = new TransactionManager(metadata);
        CommitCleaner cleaner = new CommitCleaner(transactionManager, database);

        createTestingSchema(environment);

        // drop tables and schemas
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        for (String schema : transaction.listSchemas()) {
            for (SchemaTableName tableName : transaction.listTables(schema)) {
                long tableId = transaction.getTableId(tableName).orElseThrow(AssertionError::new);
                transaction.dropTable(tableId);
            }
            transaction.dropSchema(schema);
        }
        environment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());

        // clean commits
        cleaner.removeOldCommits();

        // verify nothing exists
        MasterTestingDao master = createJdbi(database.getMasterConnection()).onDemand(MasterTestingDao.class);
        assertEquals(master.schemaCount(), 0);
        assertEquals(master.tableCount(), 0);
        assertEquals(master.columnCount(), 0);
        assertEquals(master.viewCount(), 0);

        for (Database.Shard shard : database.getShards()) {
            ShardTestingDao dao = createJdbi(shard.getConnection()).onDemand(ShardTestingDao.class);
            assertEquals(dao.chunkCount(), 0);
            assertThat(dao.tableNames()).allSatisfy(name -> assertThat(name).doesNotStartWith("x_chunks"));
        }

        // verify deletion queue size
        assertEquals(master.deletedChunksCount(), 3);
    }

    public interface MasterTestingDao
    {
        @SqlQuery("SELECT count(*) FROM schemata")
        long schemaCount();

        @SqlQuery("SELECT count(*) FROM tables")
        long tableCount();

        @SqlQuery("SELECT count(*) FROM columns")
        long columnCount();

        @SqlQuery("SELECT count(*) FROM views")
        long viewCount();

        @SqlQuery("SELECT count(*) FROM deleted_chunks")
        long deletedChunksCount();
    }

    public interface ShardTestingDao
    {
        @SqlQuery("SELECT count(*) FROM chunks")
        long chunkCount();

        @SqlQuery("SELECT table_name FROM information_schema.tables")
        List<String> tableNames();
    }
}
