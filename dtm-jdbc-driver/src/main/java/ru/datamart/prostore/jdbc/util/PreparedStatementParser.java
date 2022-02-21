/*
 * Copyright © 2021 ProStore
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
package ru.datamart.prostore.jdbc.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PreparedStatementParser {
    private static final Pattern VALUES = Pattern.compile("(?i)INSERT\\s+INTO\\s+.+VALUES\\s*\\(",
        Pattern.MULTILINE | Pattern.DOTALL);
    private final List<List<String>> parameters = new ArrayList<>();
    private final List<String> parts = new ArrayList<>();
    private boolean valuesMode = false;

    private PreparedStatementParser() {
    }

    public static PreparedStatementParser parse(String sql) {
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL may not be blank");
        } else {
            PreparedStatementParser parser = new PreparedStatementParser();
            parser.parseSQL(sql);
            return parser;
        }
    }

    public List<List<String>> getParameters() {
        return Collections.unmodifiableList(this.parameters);
    }

    public List<String> getParts() {
        return Collections.unmodifiableList(this.parts);
    }

    boolean isValuesMode() {
        return this.valuesMode;
    }

    private void reset() {
        this.parameters.clear();
        this.parts.clear();
        this.valuesMode = false;
    }

    private void parseSQL(String sql) {
        this.reset();
        List<String> currentParamList = new ArrayList<>();
        boolean afterBackSlash = false;
        boolean inQuotes = false;
        boolean inBackQuotes = false;
        boolean inSingleLineComment = false;
        boolean inMultiLineComment = false;
        boolean whiteSpace = false;
        Matcher matcher = VALUES.matcher(sql);
        if (matcher.find()) {
            this.valuesMode = true;
        }

        int currentParensLevel = 0;
        int quotedStart = 0;
        int partStart = 0;
        int i = this.valuesMode ? matcher.end() - 1 : 0;
        int idxStart = i;

        for (int idxEnd = i; i < sql.length(); ++i) {
            char c = sql.charAt(i);
            if (inSingleLineComment) {
                if (c == '\n') {
                    inSingleLineComment = false;
                }
            } else if (inMultiLineComment) {
                if (c == '*' && sql.length() > i + 1 && sql.charAt(i + 1) == '/') {
                    inMultiLineComment = false;
                    ++i;
                }
            } else if (afterBackSlash) {
                afterBackSlash = false;
            } else if (c == '\\') {
                afterBackSlash = true;
            } else if (c == '\'' && !inBackQuotes) {
                inQuotes = !inQuotes;
                if (inQuotes) {
                    quotedStart = i;
                } else {
                    idxStart = quotedStart;
                    idxEnd = i + 1;
                }
            } else if (c == '`' && !inQuotes) {
                inBackQuotes = !inBackQuotes;
            } else if (!inQuotes && !inBackQuotes) {
                if (c == '?') {
                    if (currentParensLevel > 0) {
                        idxStart = i;
                        idxEnd = i + 1;
                    }

                    if (!this.valuesMode) {
                        this.parts.add(sql.substring(partStart, i));
                        partStart = i + 1;
                        currentParamList.add("?");
                    }
                } else if (c == '-' && sql.length() > i + 1 && sql.charAt(i + 1) == '-') {
                    inSingleLineComment = true;
                    ++i;
                } else if (c == '/' && sql.length() > i + 1 && sql.charAt(i + 1) == '*') {
                    inMultiLineComment = true;
                    ++i;
                } else if (c == ',') {
                    if (this.valuesMode && idxEnd > idxStart) {
                        currentParamList.add(sql.substring(idxStart, idxEnd));
                        this.parts.add(sql.substring(partStart, idxStart));
                        partStart = idxEnd;
                        idxEnd = i;
                        idxStart = i;
                    }

                    ++idxStart;
                    ++idxEnd;
                } else if (c == '(') {
                    ++currentParensLevel;
                    ++idxStart;
                    ++idxEnd;
                } else if (c == ')') {
                    --currentParensLevel;
                    if (this.valuesMode && currentParensLevel == 0) {
                        if (idxEnd > idxStart) {
                            currentParamList.add(sql.substring(idxStart, idxEnd));
                            this.parts.add(sql.substring(partStart, idxStart));
                            partStart = idxEnd;
                            idxEnd = i;
                            idxStart = i;
                        }

                        if (!currentParamList.isEmpty()) {
                            this.parameters.add(currentParamList);
                            currentParamList = new ArrayList<>(currentParamList.size());
                        }
                    }
                } else if (Character.isWhitespace(c)) {
                    whiteSpace = true;
                } else if (currentParensLevel > 0) {
                    if (whiteSpace) {
                        idxStart = i;
                        idxEnd = i + 1;
                    } else {
                        ++idxEnd;
                    }

                    whiteSpace = false;
                }
            }
        }

        if (!this.valuesMode && !currentParamList.isEmpty()) {
            this.parameters.add(currentParamList);
        }

        String lastPart = sql.substring(partStart);
        this.parts.add(lastPart);
    }
}
