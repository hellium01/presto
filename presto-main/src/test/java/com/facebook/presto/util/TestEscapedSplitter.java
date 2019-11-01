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
import org.testng.annotations.Test;

import static org.testng.Assert.ThrowingRunnable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestEscapedSplitter
{
    @Test
    public void testSplit()
    {
        assertEquals(EscapedSplitter.on('=').splitToList("1234"), ImmutableList.of("1234"));
        assertEquals(EscapedSplitter.on('=').splitToList("1=2=3=4"), ImmutableList.of("1", "2", "3", "4"));
        assertEquals(EscapedSplitter.on('=').splitToList("1=2=3=4="), ImmutableList.of("1", "2", "3", "4", ""));
        assertEquals(EscapedSplitter.on('=').splitToList("1==23=4="), ImmutableList.of("1", "", "23", "4", ""));
    }

    @Test
    public void testTrim()
    {
        assertEquals(EscapedSplitter.on('=').trimResults().splitToList(" 1234 "), ImmutableList.of("1234"));
        assertEquals(EscapedSplitter.on('=').trimResults().splitToList(" 1=2= 3=4 "), ImmutableList.of("1", "2", "3", "4"));
        assertEquals(EscapedSplitter.on('=').trimResults().splitToList("1 =2  =3 =4= "), ImmutableList.of("1", "2", "3", "4", ""));
        assertEquals(EscapedSplitter.on('=').trimResults().splitToList(" 1== 2 3 = 4="), ImmutableList.of("1", "", "2 3", "4", ""));
    }

    @Test
    public void testOmitEmptyStrings()
    {
        assertEquals(EscapedSplitter.on('=').omitEmptyStrings().splitToList("1234"), ImmutableList.of("1234"));
        assertEquals(EscapedSplitter.on('=').omitEmptyStrings().splitToList("1=2=3=4"), ImmutableList.of("1", "2", "3", "4"));
        assertEquals(EscapedSplitter.on('=').omitEmptyStrings().splitToList("1=2=3=4="), ImmutableList.of("1", "2", "3", "4"));
        assertEquals(EscapedSplitter.on('=').omitEmptyStrings().splitToList("1==23=4="), ImmutableList.of("1", "23", "4"));

        assertEquals(EscapedSplitter.on('=').trimResults().omitEmptyStrings().splitToList(" 1234 "), ImmutableList.of("1234"));
        assertEquals(EscapedSplitter.on('=').omitEmptyStrings().trimResults().splitToList(" 1=2= 3=4 "), ImmutableList.of("1", "2", "3", "4"));
        assertEquals(EscapedSplitter.on('=').trimResults().omitEmptyStrings().splitToList("1 =2  =3 =4= "), ImmutableList.of("1", "2", "3", "4"));
        assertEquals(EscapedSplitter.on('=').omitEmptyStrings().trimResults().splitToList(" 1=   = 2 3 = 4="), ImmutableList.of("1", "2 3", "4"));
    }

    @Test
    public void testEscape()
    {
        assertEquals(EscapedSplitter.on('=').splitToList("1='2=3'"), ImmutableList.of("1", "'2", "3'"));
        assertEquals(EscapedSplitter.on('=').splitToList("1=''2=3'"), ImmutableList.of("1", "''2", "3'"));

        assertEquals(EscapedSplitter.on('=').escapeQuoted('\'').splitToList("1='2=3'"), ImmutableList.of("1", "'2=3'"));
        assertEquals(EscapedSplitter.on('=').escapeQuoted('\'').trimResults(CharMatcher.is('\'')).splitToList("1='2===3'"), ImmutableList.of("1", "2===3"));
        assertEquals(EscapedSplitter.on('=').escapeQuoted('\'').splitToList("1='2\\'=3'"), ImmutableList.of("1", "'2\\'=3'"));
        assertEquals(EscapedSplitter.on('=').trimResults(CharMatcher.is('\'')).escapeQuoted('\'').splitToList("1='2=3'"), ImmutableList.of("1", "2=3"));
        assertEquals(EscapedSplitter.on('=').escapeQuoted('\'').trimResults(CharMatcher.is('\'')).splitToList("1='2\\'=3'"), ImmutableList.of("1", "2\\'=3"));

        assertEquals(EscapedSplitter.on('=').escapeQuoted(CharMatcher.anyOf("\"\'")).splitToList("1='\"2\"=\"3'"), ImmutableList.of("1", "'\"2\"=\"3'"));
        assertEquals(EscapedSplitter.on('=').escapeQuoted(CharMatcher.anyOf("\"\'")).splitToList("1='\"2\"=\"3'=\"'4'\""), ImmutableList.of("1", "'\"2\"=\"3'", "\"'4'\""));
    }

    @Test
    public void testFailedEscape()
    {
        assertThrows("escaped quote character ' found at unquoted location: 3", () -> EscapedSplitter.on('=').escapeQuoted('\'').splitToList("1=\\'2\\'=3'"));
        assertThrows("escaped quote character ' found at unquoted location: 10", () -> EscapedSplitter.on('=').escapeQuoted('\'').splitToList("1='2\\'=3'\\'"));
        assertThrows("escaped quote character ' found at unquoted location: 6", () -> EscapedSplitter.on('=').escapeQuoted('\'').splitToList("1=''2\\'=3'\\'"));
        assertThrows("unmatched quote character ' found at location: 13", () -> EscapedSplitter.on('=').escapeQuoted('\'').splitToList("1=''2'\\'=3\\'''"));
        assertThrows("unmatched quote character \" found at location: 13", () -> EscapedSplitter.on('=').escapeQuoted(CharMatcher.anyOf("\"\'")).splitToList("1=''2'\\'=3\\''\""));
    }

    private static void assertThrows(String message, ThrowingRunnable runnable)
    {
        try {
            runnable.run();
            fail("expected exception");
        }
        catch (Throwable t) {
            assertEquals(t.getMessage(), message);
        }
    }
}
