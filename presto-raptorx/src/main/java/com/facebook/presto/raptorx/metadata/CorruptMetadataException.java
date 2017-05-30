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

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CORRUPT_METADATA;

public class CorruptMetadataException
        extends PrestoException
{
    public CorruptMetadataException(String message)
    {
        super(RAPTOR_CORRUPT_METADATA, message);
    }
}
