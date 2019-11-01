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
package com.facebook.presto.util;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * An extension of Guava Splitter to escape in quoted string. For example:
 * EscapedSplitter.on("=").escapeQuoted('\'').splitToList("a='b=c'") will
 * be splitted into ["a", "'b=c'"]
 */
public class EscapedSplitter
{
    private enum State
    {
        READY,
        NOT_READY,
        DONE,
        FAILED,
    }

    private final Strategy strategy;
    private final CharMatcher trimmer;
    private final boolean omitEmptyStrings;
    private final int limit;

    private EscapedSplitter(Strategy strategy)
    {
        this(strategy, false, CharMatcher.none(), Integer.MAX_VALUE);
    }

    private EscapedSplitter(Strategy strategy, boolean omitEmptyStrings, CharMatcher trimmer, int limit)
    {
        this.strategy = requireNonNull(strategy, "strategy is null");
        this.omitEmptyStrings = omitEmptyStrings;
        this.trimmer = requireNonNull(trimmer, "trimer is null");
        this.limit = limit;
    }

    private interface Strategy
    {
        int separatorStart(final CharSequence toSplit, int start);

        int separatorEnd(int separatorPosition);
    }

    public static EscapedSplitter on(Character separator)
    {
        return on(CharMatcher.is(separator));
    }

    public static EscapedSplitter on(final CharMatcher separatorMatcher)
    {
        requireNonNull(separatorMatcher, "separatorMatcher is null");
        return new EscapedSplitter(new Strategy()
        {
            @Override
            public int separatorStart(final CharSequence toSplit, int start)
            {
                return separatorMatcher.indexIn(toSplit, start);
            }

            @Override
            public int separatorEnd(int separatorPosition)
            {
                return separatorPosition + 1;
            }
        });
    }

    public EscapedSplitter trimResults()
    {
        return trimResults(CharMatcher.whitespace());
    }

    public EscapedSplitter trimResults(CharMatcher trimmer)
    {
        return new EscapedSplitter(strategy, omitEmptyStrings, trimmer, limit);
    }

    public EscapedSplitter omitEmptyStrings()
    {
        return new EscapedSplitter(strategy, true, trimmer, limit);
    }

    public List<String> splitToList(CharSequence sequence)
    {
        Iterator<String> iterator = new SplittingIterator(sequence);
        ImmutableList.Builder<String> result = ImmutableList.builder();

        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        return result.build();
    }

    public EscapedSplitter escapeQuoted(Character quote)
    {
        return escapeQuoted(CharMatcher.is(quote));
    }

    public EscapedSplitter escapeQuoted(CharMatcher quoteMatcher)
    {
        requireNonNull(quoteMatcher, "quoteMatcher is null");
        return new EscapedSplitter(new Strategy()
        {
            @Override
            public int separatorStart(CharSequence toSplit, int start)
            {
                while (start != -1 && start < toSplit.length()) {
                    int separatorStart = strategy.separatorStart(toSplit, start);
                    int quoteStart = quoteMatcher.indexIn(toSplit, start);
                    if (quoteStart < 0) {
                        return separatorStart;
                    }
                    Character quote = toSplit.charAt(quoteStart);
                    checkArgument(!isEscaped(toSplit, quoteStart), format("escaped quote character %s found at unquoted location: %s", toSplit.charAt(quoteStart), quoteStart));
                    int quoteEnd = CharMatcher.is(quote).indexIn(toSplit, quoteStart + 1);
                    while (quoteEnd != -1 && isEscaped(toSplit, quoteEnd)) {
                        quoteEnd = CharMatcher.is(quote).indexIn(toSplit, quoteEnd + 1);
                    }
                    checkArgument(quoteEnd > quoteStart, format("unmatched quote character %s found at location: %s", toSplit.charAt(quoteStart), quoteStart));
                    // return after making sure quote is complete
                    if (separatorStart < quoteStart) {
                        return separatorStart;
                    }

                    start = quoteEnd + 1;
                }
                return -1;
            }

            private boolean isEscaped(CharSequence string, int quoteLocation)
            {
                return quoteLocation >= 1 && string.charAt(quoteLocation - 1) == '\\';
            }

            @Override
            public int separatorEnd(int separatorPosition)
            {
                return strategy.separatorEnd(separatorPosition);
            }
        }, this.omitEmptyStrings, this.trimmer, this.limit);
    }

    private class SplittingIterator
            implements Iterator<String>
    {
        private final CharSequence toSplit;
        private @Nullable String next;
        private State state;
        private int offset;
        private int limit;

        public SplittingIterator(CharSequence toSplit)
        {
            this.toSplit = toSplit;
            this.limit = EscapedSplitter.this.limit;
            this.state = State.NOT_READY;
        }

        @Nullable
        private String endOfData()
        {
            state = State.DONE;
            return null;
        }

        @Override
        public final boolean hasNext()
        {
            checkState(state != State.FAILED);
            switch (state) {
                case READY:
                    return true;
                case DONE:
                    return false;
                default:
            }
            return tryToComputeNext();
        }

        private boolean tryToComputeNext()
        {
            state = State.FAILED; // temporary pessimism
            next = computeNext();
            if (state != State.DONE) {
                state = State.READY;
                return true;
            }
            return false;
        }

        private String computeNext()
        {
            /*
             * The returned string will be from the end of the last match to the beginning of the next
             * one. nextStart is the start position of the returned substring, while offset is the place
             * to start looking for a separator.
             */
            int nextStart = offset;
            while (offset != -1) {
                int start = nextStart;
                int end;

                int separatorPosition = strategy.separatorStart(toSplit, offset);
                if (separatorPosition == -1) {
                    end = toSplit.length();
                    offset = -1;
                }
                else {
                    end = separatorPosition;
                    offset = strategy.separatorEnd(separatorPosition);
                }
                if (offset == nextStart) {
                    /*
                     * This occurs when some pattern has an empty match, even if it doesn't match the empty
                     * string -- for example, if it requires lookahead or the like. The offset must be
                     * increased to look for separators beyond this point, without changing the start position
                     * of the next returned substring -- so nextStart stays the same.
                     */
                    offset++;
                    if (offset > toSplit.length()) {
                        offset = -1;
                    }
                    continue;
                }

                while (start < end && trimmer.matches(toSplit.charAt(start))) {
                    start++;
                }
                while (end > start && trimmer.matches(toSplit.charAt(end - 1))) {
                    end--;
                }

                if (omitEmptyStrings && start == end) {
                    // Don't include the (unused) separator in next split string.
                    nextStart = offset;
                    continue;
                }

                if (limit == 1) {
                    // The limit has been reached, return the rest of the string as the
                    // final item. This is tested after empty string removal so that
                    // empty strings do not count towards the limit.
                    end = toSplit.length();
                    offset = -1;
                    // Since we may have changed the end, we need to trim it again.
                    while (end > start && trimmer.matches(toSplit.charAt(end - 1))) {
                        end--;
                    }
                }
                else {
                    limit--;
                }

                return toSplit.subSequence(start, end).toString();
            }
            return endOfData();
        }

        @Override
        public final String next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            state = State.NOT_READY;
            String result = next;
            next = null;
            return result;
        }
    }
}
