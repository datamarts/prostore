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
package ru.datamart.prostore.query.execution.plugin.adb.dml.factory;

import lombok.val;

import java.util.List;
import java.util.stream.Collectors;

public class AdbUpsertSqlFactory {

    private static final String STAGING_COLUMN_PREF = "staging.";
    private static final String OLD_COLUMN_PREF = "old.";
    private static final String STAGING_ACTUAL_EQUAL_CONDITION = "staging.%s = actual.%s";
    private static final String OLD_STAGING_EQUAL_CONDITION = "old.%s = staging.%s";

    private static final String UPDATE = "UPDATE %s_actual actual\n" +
            "SET \n" +
            "  sys_to = %d,\n" +
            "  sys_op = staging.sys_op\n" +
            "FROM (\n" +
            "  SELECT %s, MAX(sys_op) as sys_op\n" +
            "  FROM %s_staging\n" +
            "  GROUP BY %s\n" +
            "    ) staging\n" +
            "WHERE %s \n" +
            "  AND actual.sys_from < %d\n" +
            "  AND actual.sys_to IS NULL;";

    private static final String INSERT = "INSERT INTO %s_actual (%s, sys_from, sys_op)\n" +
            "  SELECT DISTINCT ON (%s) %s, %d AS sys_from, 0 AS sys_op \n" +
            "  FROM %s_staging staging\n" +
            "    LEFT JOIN %s_actual old\n" +
            "      ON %s AND old.sys_to = %d   \n" +
            "    LEFT JOIN %s_actual actual \n" +
            "      ON %s AND actual.sys_from = %d\n" +
            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;";

    private static final String TRUNCATE = "TRUNCATE %s_staging;";
    private static final String WHERE_DELIMITER = " AND ";
    private static final String COLUMNS_DELIMETER = ", ";

    private AdbUpsertSqlFactory() {
    }

    public static String updateSql(String nameWithSchema, List<String> entityPkFields, Long sysCn) {
        val pkFieldsString = String.join(", ", entityPkFields);
        val equalityCondition = entityPkFields.stream()
                .map(field -> String.format(STAGING_ACTUAL_EQUAL_CONDITION, field, field))
                .collect(Collectors.joining(WHERE_DELIMITER));
        return String.format(UPDATE, nameWithSchema,
                sysCn - 1,
                pkFieldsString,
                nameWithSchema,
                pkFieldsString,
                equalityCondition,
                sysCn);
    }

    public static String insertSql(String nameWithSchema, List<String> entityFields, List<String> entityPkFields, List<String> targetColumnList, Long sysCn) {
        val oldStagingEqualityCondition = entityPkFields.stream()
                .map(field -> String.format(OLD_STAGING_EQUAL_CONDITION, field, field))
                .collect(Collectors.joining(WHERE_DELIMITER));
        val stagingActualEqualityCondition = entityPkFields.stream()
                .map(field -> String.format(STAGING_ACTUAL_EQUAL_CONDITION, field, field))
                .collect(Collectors.joining(WHERE_DELIMITER));
        val oldAndStagingColumns = entityFields.stream()
                .map(field -> {
                    if (targetColumnList.contains(field)) {
                        return STAGING_COLUMN_PREF + field;
                    }

                    return OLD_COLUMN_PREF + field;
                })
                .collect(Collectors.toList());

        val stagingPkFields = entityPkFields.stream()
                .map(field -> STAGING_COLUMN_PREF + field)
                .collect(Collectors.joining(COLUMNS_DELIMETER));
        return String.format(INSERT, nameWithSchema, String.join(", ", entityFields),
                stagingPkFields, String.join(COLUMNS_DELIMETER, oldAndStagingColumns), sysCn,
                nameWithSchema,
                nameWithSchema,
                oldStagingEqualityCondition, sysCn - 1,
                nameWithSchema,
                stagingActualEqualityCondition, sysCn);
    }

    public static String truncateSql(String nameWithSchema) {
        return String.format(TRUNCATE, nameWithSchema);
    }
}
