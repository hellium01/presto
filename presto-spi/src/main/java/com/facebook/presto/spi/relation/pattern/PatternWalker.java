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

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PatternWalker<T extends Node<T>, R>
{
    private final BiFunction<T, Function<T, R>, Optional<R>> binder;

    private PatternWalker(BiFunction<T, Function<T, R>, Optional<R>> next)
    {
        this.binder = next;
    }

    public R accept(T obj)
    {
        List<R> results = obj.getChildren().stream()
                .map(child -> accept(child))
                .collect(Collectors.toList());
        return binder.apply(obj, null).get();
    }

    public <Y> PatternWalker with(Pattern<Y> pattern, final Function<Y, R> consumer)
    {
        return new PatternWalker((obj, next) -> {
            Optional<R> result = binder.apply((T) obj, (Function<T, R>) next);
            if (result.isPresent()) {
                return result;
            }
            if (pattern.getType().isAssignableFrom(obj.getClass()) && pattern.match((Y) obj)) {
                final Y as = (Y) obj;
                return Optional.of(consumer.apply(as));
            }
            return Optional.empty();
        });
    }

    public PatternWalker<T, R> orElse(final Function<T, R> consumer)
    {
        return new PatternWalker<T, R>((obj, next) -> {
            Optional<R> result = binder.apply((T) obj, (Function<T, R>) next);
            if (result.isPresent()) {
                return result;
            }
            return Optional.of(consumer.apply((T) obj));
        });
    }

    public static PatternWalker match()
    {
        return new PatternWalker((a, b) -> Optional.empty());
    }
}
