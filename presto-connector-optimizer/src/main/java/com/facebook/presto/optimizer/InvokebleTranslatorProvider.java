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
package com.facebook.presto.optimizer;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;

import java.lang.invoke.MethodHandle;
import java.util.Map;

public class InvokebleTranslatorProvider<T>
        implements FunctionTranslatorProvider<T>
{
    private final FunctionMetadataManager metadataManager;
    private final Map<FunctionMetadata, MethodHandle> methodHandles;

    public InvokebleTranslatorProvider(FunctionMetadataManager metadataManager)
    {
        this.metadataManager = metadataManager;
    }

    @Override
    public FunctionTranslator<T> getFunctionTranslator(FunctionHandle functionHandle)
    {
        FunctionMetadata metadata = metadataManager.getFunctionMetadata(functionHandle);
        MethodHandle methodHandle = methodHandles.get(metadata);
        if (methodHandle == null) {
            return null;
        }
        return new InvokableTranslator<>(methodHandle);
    }
}
