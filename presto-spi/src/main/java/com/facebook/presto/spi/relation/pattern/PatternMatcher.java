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

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PatternMatcher<T, R>
{
    private final BiFunction<T, Function<T, R>, Optional<R>> binder;

    private PatternMatcher(BiFunction<T, Function<T, R>, Optional<R>> next)
    {
        this.binder = next;
    }

    public void accept(T obj)
    {
        binder.apply(obj, null);
    }

    public <Y> PatternMatcher with(Pattern<Y> pattern, final Function<Y, R> consumer)
    {
        return new PatternMatcher((obj, next) -> {
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

    public PatternMatcher<T, R> orElse(final Function<T, R> consumer)
    {
        return new PatternMatcher<T, R>((obj, next) -> {
            Optional<R> result = binder.apply((T) obj, (Function<T, R>) next);
            if (result.isPresent()) {
                return result;
            }
            return Optional.of(consumer.apply((T) obj));
        });
    }

    public static PatternMatcher match()
    {
        return new PatternMatcher((a, b) -> Optional.empty());
    }
}
