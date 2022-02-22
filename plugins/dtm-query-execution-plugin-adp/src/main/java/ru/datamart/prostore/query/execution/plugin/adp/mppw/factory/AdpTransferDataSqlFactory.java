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
package ru.datamart.prostore.query.execution.plugin.adp.mppw.factory;

import lombok.val;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class AdpTransferDataSqlFactory {

    private static final String CLOSE_VERSION_OF_RECORDS_TEMPLATE = "UPDATE ${datamart}.${tableName}_actual actual\n" +
            "SET \n" +
            "  sys_to = ${previousSysCn},\n" +
            "  sys_op = staging.sys_op\n" +
            "FROM (\n" +
            "  SELECT ${primaryKeys}, MAX(sys_op) as sys_op\n" +
            "  FROM ${datamart}.${tableName}_staging\n" +
            "  GROUP BY ${primaryKeys}\n" +
            "    ) staging\n" +
            "WHERE ${primaryKeysEquity} \n" +
            "  AND actual.sys_from < ${sysCn}\n" +
            "  AND actual.sys_to IS NULL;";

    private static final String UPLOAD_HOT_RECORDS_TEMPLATE = "INSERT INTO ${datamart}.${tableName}_actual (${allFields}, sys_from, sys_op)\n" +
            "  SELECT DISTINCT ON (${primaryKeysStaging}) ${allFieldsStaging}, ${sysCn} AS sys_from, 0 AS sys_op \n" +
            "  FROM ${datamart}.${tableName}_staging staging\n" +
            "    LEFT JOIN ${datamart}.${tableName}_actual actual \n" +
            "    ON ${primaryKeysEquity} AND actual.sys_from = ${sysCn}\n" +
            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;";

    private static final String TRUNCATE_TABLE_TEMPLATE = "TRUNCATE %s.%s_staging;";

    public String getCloseVersionOfRecordsSql(String datamart, String tableName,
                                              List<String> primaryKeys, Long sysCn) {
        val currentSysCn = Long.toString(sysCn);
        val previousSysCn = Long.toString(sysCn - 1L);

        String primaryKeyList = String.join(", ", primaryKeys);

        String primaryKeyEquity = primaryKeys.stream()
                .map(fieldName -> String.format("actual.%s = staging.%s", fieldName, fieldName))
                .collect(Collectors.joining(" AND "));

        return CLOSE_VERSION_OF_RECORDS_TEMPLATE
                .replace("${datamart}", datamart)
                .replace("${tableName}", tableName)
                .replace("${previousSysCn}", previousSysCn)
                .replace("${sysCn}", currentSysCn)
                .replace("${primaryKeys}", primaryKeyList)
                .replace("${primaryKeysEquity}", primaryKeyEquity);
    }

    public String getUploadHotRecordsSql(String datamart, String tableName,
                                         List<String> allFields, List<String> primaryKeys, Long sysCn) {
        val currentSysCn = Long.toString(sysCn);

        val allFieldsList = String.join(", ", allFields);
        val allFieldsListStaging = allFields.stream()
                .map(fieldName -> String.format("staging.%s", fieldName))
                .collect(Collectors.joining(", "));

        val primaryKeysStaging = primaryKeys.stream()
                .map(fieldName -> String.format("staging.%s", fieldName))
                .collect(Collectors.joining(", "));

        String primaryKeyEquity = primaryKeys.stream()
                .map(fieldName -> String.format("actual.%s = staging.%s", fieldName, fieldName))
                .collect(Collectors.joining(" AND "));

        return UPLOAD_HOT_RECORDS_TEMPLATE
                .replace("${datamart}", datamart)
                .replace("${tableName}", tableName)
                .replace("${allFields}", allFieldsList)
                .replace("${primaryKeysStaging}", primaryKeysStaging)
                .replace("${allFieldsStaging}", allFieldsListStaging)
                .replace("${sysCn}", currentSysCn)
                .replace("${primaryKeysEquity}", primaryKeyEquity);
    }

    public String getTruncateSql(String datamart, String tableName) {
        return String.format(TRUNCATE_TABLE_TEMPLATE, datamart, tableName);
    }

}
