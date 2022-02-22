/*
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.jdbc.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SqlParser {

    public static List<Query> parseSql(String query) throws SQLException {
        int inParen = 0;
        char[] aChars = query.toCharArray();
        boolean whitespaceOnly = true;
        List<Query> nativeQueries = null;
        StringBuilder nativeSql = new StringBuilder();
        int fragmentStart = 0;
        if (query.isEmpty()) {
            return Collections.singletonList(new Query(query, true));
        }

        int i = 0;
        while (i < aChars.length) {
            char aChar = aChars[i];
            whitespaceOnly &= aChar == ';' || Character.isWhitespace(aChar);
            switch (aChar) {
                case '"':
                    i = parseDoubleQuotes(aChars, i);
                    break;
                case '\'':
                    i = parseSingleQuotes(aChars, i);
                    break;
                case '(':
                    ++inParen;
                    break;
                case ')':
                    --inParen;
                    break;
                case ';':
                    whitespaceOnly = appendInParenCharsAndCheckWhitespace(inParen,
                            aChars, whitespaceOnly, nativeSql, fragmentStart, i);
                    fragmentStart = i + 1;
                    if (nativeQueries == null) {
                        nativeQueries = new ArrayList<>();
                    }
                    nativeQueries.add(new Query(nativeSql.toString(), false));
                    nativeSql.setLength(0);
                    break;
                default:
                    break;
            }
            ++i;
        }
        return getQueries(aChars, whitespaceOnly, nativeQueries, nativeSql, fragmentStart);
    }

    private static boolean appendInParenCharsAndCheckWhitespace(int inParen,
                                                                char[] aChars,
                                                                boolean whitespaceOnly,
                                                                StringBuilder nativeSql,
                                                                int fragmentStart,
                                                                int i) throws SQLException {
        if (inParen == 0) {
            if (!whitespaceOnly) {
                nativeSql.append(aChars, fragmentStart, i - fragmentStart);
                return true;
            }
        } else {
            throw new SQLException(String.format("Invalid sql query %s", Arrays.toString(aChars)));
        }
        return false;
    }

    private static List<Query> getQueries(char[] aChars,
                                          boolean whitespaceOnly,
                                          List<Query> nativeQueries,
                                          StringBuilder nativeSql,
                                          int fragmentStart) {
        if (fragmentStart < aChars.length && !whitespaceOnly) {
            nativeSql.append(aChars, fragmentStart, aChars.length - fragmentStart);
        }

        if (nativeSql.length() == 0) {
            return nativeQueries != null ? nativeQueries : Collections.emptyList();
        } else {
            return getQueriesIfNativeSqlNonEmpty(whitespaceOnly, nativeQueries, nativeSql);
        }
    }

    private static List<Query> getQueriesIfNativeSqlNonEmpty(boolean whitespaceOnly,
                                                             List<Query> nativeQueries,
                                                             StringBuilder nativeSql) {
        Query lastQuery = new Query(nativeSql.toString(), false);
        if (nativeQueries == null) {
            return Collections.singletonList(lastQuery);
        } else {
            if (!whitespaceOnly) {
                nativeQueries.add(lastQuery);
            }
            return nativeQueries;
        }
    }

    private static int parseSingleQuotes(char[] query, int offset) {
        while (true) {
            ++offset;
            if (offset >= query.length) {
                break;
            }

            if (query[offset] == '\'') {
                return offset;
            }
        }
        return query.length;
    }

    private static int parseDoubleQuotes(char[] query, int offset) {
        do {
            ++offset;
        } while (offset < query.length && query[offset] != '"');

        return offset;
    }
}
