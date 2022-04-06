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
package ru.datamart.prostore.query.calcite.core.extension.eddl;

import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OptionsOperator extends SqlCall {

    private static final SqlOperator OPTIONS_OPERATOR =
            new SqlSpecialOperator("OPTIONS", SqlKind.OTHER_DDL);
    private static final String SEMICOLON_DELIMITER = ";";
    private static final String EQUAL_DELIMITER = "=";
    private final Map<String, String> optionsMap;

    public OptionsOperator(SqlParserPos pos, SqlNode options) {
        super(pos);
        this.optionsMap = parseOptions(options);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPTIONS_OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    public Map<String, String> getOptionsMap() {
        return optionsMap;
    }

    public String getOption(String option) {
        return optionsMap.get(option);
    }

    public boolean isEmpty() {
        return optionsMap.isEmpty();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        val optionsString = optionsMap.entrySet().stream()
                .map(e -> e.getKey() + EQUAL_DELIMITER + e.getValue())
                .collect(Collectors.joining(SEMICOLON_DELIMITER));
        writer.literal("('" + optionsString + "')");
    }

    private Map<String, String> parseOptions(SqlNode options) {
        if (options == null) {
            return Collections.emptyMap();
        }
        val optionsString = ((SqlCharStringLiteral) options).getNlsString().getValue();
        if (optionsString.isEmpty()) {
            return Collections.emptyMap();
        }

        val pairs = optionsString.split(SEMICOLON_DELIMITER);
        return Arrays.stream(pairs)
                .map(pair -> {
                    val keyValue = pair.split(EQUAL_DELIMITER);
                    if (keyValue.length != 2) {
                        throw new IllegalArgumentException("Incorrect OPTIONS value: " + options);
                    }
                    return keyValue;
                })
                .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
    }
}
