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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class TransactionManager
{
    private final Metadata metadata;

    private final Map<ConnectorTransactionHandle, Transaction> transactions = new ConcurrentHashMap<>();

    @Inject
    public TransactionManager(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public ConnectorTransactionHandle create()
    {
        long transactionId = metadata.nextTransactionId();
        long commitId = metadata.getCurrentCommitId();
        Transaction transaction = new Transaction(metadata, transactionId, commitId);

        RaptorTransactionHandle handle = new RaptorTransactionHandle(transactionId, commitId);
        transactions.put(handle, transaction);
        return handle;
    }

    public Transaction get(ConnectorTransactionHandle handle)
    {
        Transaction transaction = transactions.get(handle);
        if (transaction == null) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "No such transaction: " + handle);
        }
        return transaction;
    }

    public Transaction remove(ConnectorTransactionHandle handle)
    {
        Transaction transaction = transactions.remove(handle);
        if (transaction == null) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "No such transaction: " + handle);
        }
        return transaction;
    }

    public long oldestActiveCommitId()
    {
        return transactions.values().stream()
                .mapToLong(Transaction::getCommitId)
                .min()
                .orElseGet(metadata::getCurrentCommitId);
    }
}
