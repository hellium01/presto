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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.UpdatablePageSource;

import java.util.List;

public interface PageSourceProvider
{
    ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns);

    default UpdatablePageSource createUpdatablePageSource(Session session, Split split, List<ColumnHandle> columns)
    {
        throw new UnsupportedOperationException();
    }
}
