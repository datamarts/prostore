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
package ru.datamart.prostore.query.execution.plugin.adp.dml.factory;

import lombok.val;
import ru.datamart.prostore.common.model.ddl.EntityField;

import java.util.List;
import java.util.stream.Collectors;

public class AdpStandaloneUpsertSqlFactory {

    private static final String NON_PK_EQUALS_EXCLUDED = "%s = EXCLUDED.%s";
    private static final String INSERT_INTO = "INSERT INTO %s AS dest (%s) %s \n" +
            "ON CONFLICT (%s) \n" +
            "DO %s";
    private static final String NOTHING = "NOTHING";
    private static final String UPDATE_SET = "UPDATE SET %s";

    private AdpStandaloneUpsertSqlFactory() {
    }

    public static String insertSql(String destination, List<EntityField> targetColumns, String valuesString, List<String> primaryKeys) {
        val targetColumnString = targetColumns.stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        val exludedNonPkEqualsCondition = targetColumns.stream()
                .map(EntityField::getName)
                .filter(column -> !primaryKeys.contains(column))
                .map(column -> String.format(NON_PK_EQUALS_EXCLUDED, column, column))
                .collect(Collectors.joining(", "));
        val doCondition = exludedNonPkEqualsCondition.isEmpty()
                ? NOTHING
                : String.format(UPDATE_SET, exludedNonPkEqualsCondition);
        return String.format(INSERT_INTO, destination, targetColumnString, valuesString, String.join(", ", primaryKeys), doCondition);
    }
}
