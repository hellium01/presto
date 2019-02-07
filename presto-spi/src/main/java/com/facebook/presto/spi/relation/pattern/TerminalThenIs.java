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
package com.facebook.presto.spi.relation.pattern;

import java.util.function.Consumer;

class TerminalThenIs<S, T> extends ThenIs<S, T> {

    TerminalThenIs(Then<S> parent, S object, Class<T> expectedType) {
        super(parent, object, expectedType);
    }

    @Override
    public Then<S> then(Consumer<T> thenBlock) {
        return parent;
    }
}
