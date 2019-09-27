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

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class InvokableTranslator<T>
        implements FunctionTranslator<T>
{
    private final MethodHandle methodHandle;

    public InvokableTranslator(MethodHandle methodHandle)
    {
        this.methodHandle = requireNonNull(methodHandle);
    }

    @Override
    public Optional<T> translate(List<TranslatedExpression<T>> arguments)
    {
        try {
            return Optional.of((T) methodHandle.invoke(arguments));
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(t);
        }
    }
}
