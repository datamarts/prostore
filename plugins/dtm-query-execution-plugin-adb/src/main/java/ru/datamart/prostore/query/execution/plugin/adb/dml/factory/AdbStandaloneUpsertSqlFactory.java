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
package ru.datamart.prostore.query.execution.plugin.adb.dml.factory;

import lombok.val;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class AdbStandaloneUpsertSqlFactory {

    public static final String TEMP_TABLE_NAME = "tmp_%s";

    private static final String CREATE_TEMP_TABLE = "CREATE TEMP TABLE %s (%s) \n" +
            "ON COMMIT DROP%s";
    private static final String DISTRIBUTED_BY = " DISTRIBUTED BY (%s)";

    private static final String INSERT_INTO_TEMP = "INSERT INTO %s (%s) %s";

    private static final String COLUMN_EQUAL_TMP = "%s = tmp.%s";
    private static final String COLUMN_DST_EQUAL_TMP = "dst.%s = tmp.%s";
    private static final String UPDATE = "UPDATE %s dst\n" +
            "  SET \n" +
            "    %s" +
            "  FROM %s tmp\n" +
            "  WHERE %s;";

    private static final String TMP_PREF = "tmp.%s";
    private static final String INSERT_INTO_DESTINATION = "INSERT INTO %s (%s)\n" +
            "    SELECT %s \n" +
            "    FROM %s tmp\n" +
            "      LEFT JOIN %s dst ON %s\n" +
            "    WHERE dst.%s IS null;";

    private AdbStandaloneUpsertSqlFactory() {
    }

    public static String tempTableName(UUID requestId) {
        return String.format(TEMP_TABLE_NAME, requestId).replace("-", "_");
    }

    public static String createTempTableSql(List<EntityField> targetColumns, List<String> shardingKeys, String tempTable) {
        val columnList = targetColumns.stream()
                .map(column -> column.getName() + " " + AdbTypeUtil.adbTypeFromDtmType(column))
                .collect(Collectors.joining(", "));
        val distributedBy = shardingKeys.isEmpty() ? "" : String.format(DISTRIBUTED_BY, String.join(", ", shardingKeys));
        return String.format(CREATE_TEMP_TABLE, tempTable, columnList, distributedBy);
    }

    public static String insertIntoTempSql(List<EntityField> targetColumns, String tempTable, String values) {
        val columnList = targetColumns.stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        return String.format(INSERT_INTO_TEMP, tempTable, columnList, values);
    }

    public static String updateSql(List<EntityField> targetColumns, List<String> primaryKeys, String destination, String tempTable) {
        val nonPkEquals = targetColumns.stream()
                .map(EntityField::getName)
                .filter(name -> !primaryKeys.contains(name))
                .map(column -> String.format(COLUMN_EQUAL_TMP, column, column))
                .collect(Collectors.joining(", "));

        if (nonPkEquals.isEmpty()) {
            return null;
        }

        val condition = pkEqualsCondition(primaryKeys);
        return String.format(UPDATE, destination, nonPkEquals, tempTable, condition);
    }

    public static String insertIntoDestinationSql(List<EntityField> targetColumns, String destination, String tempTable, List<String> primaryKeys) {
        val targetColumnString = targetColumns.stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        val tmpTargetColumnString = targetColumns.stream()
                .map(column -> String.format(TMP_PREF, column.getName()))
                .collect(Collectors.joining(", "));
        val condition = pkEqualsCondition(primaryKeys);
        return String.format(INSERT_INTO_DESTINATION, destination, targetColumnString, tmpTargetColumnString, tempTable, destination, condition, primaryKeys.get(0));
    }

    private static String pkEqualsCondition(List<String> primaryKeys) {
        return primaryKeys.stream()
                .map(column -> String.format(COLUMN_DST_EQUAL_TMP, column, column))
                .collect(Collectors.joining(" AND "));
    }
}
