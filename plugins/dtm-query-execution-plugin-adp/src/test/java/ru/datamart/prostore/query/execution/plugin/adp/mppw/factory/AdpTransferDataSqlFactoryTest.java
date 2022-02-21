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
package ru.datamart.prostore.query.execution.plugin.adp.mppw.factory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class AdpTransferDataSqlFactoryTest {
    private static final String DATAMART = "datamart";
    public static final long SYS_CN = 10L;
    public static final String TABLE_NAME = "adpTable";
    public static final List<String> PRIMARY_KEYS = Arrays.asList("id", "pk2");
    public static final List<String> ALL_FIELDS = Arrays.asList("id", "pk2", "col_1", "col_2");

    private final AdpTransferDataSqlFactory sqlFactory = new AdpTransferDataSqlFactory();

    @Test
    void shouldBeCorrectCloseVersionOfRecordsSql() {
        // act
        String closeVersionOfRecordsSql = sqlFactory.getCloseVersionOfRecordsSql(DATAMART, TABLE_NAME, PRIMARY_KEYS, SYS_CN);

        // assert
        Assertions.assertThat(closeVersionOfRecordsSql).isEqualToNormalizingNewlines("UPDATE datamart.adpTable_actual actual\n" +
                "SET \n" +
                "  sys_to = 9,\n" +
                "  sys_op = staging.sys_op\n" +
                "FROM (\n" +
                "  SELECT id, pk2, MAX(sys_op) as sys_op\n" +
                "  FROM datamart.adpTable_staging\n" +
                "  GROUP BY id, pk2\n" +
                "    ) staging\n" +
                "WHERE actual.id = staging.id AND actual.pk2 = staging.pk2 \n" +
                "  AND actual.sys_from < 10\n" +
                "  AND actual.sys_to IS NULL;");
    }

    @Test
    void shouldBeCorrectUploadHotRecordsSql() {
        // act
        String closeVersionOfRecordsSql = sqlFactory.getUploadHotRecordsSql(DATAMART, TABLE_NAME, ALL_FIELDS, PRIMARY_KEYS, SYS_CN);

        // assert
        Assertions.assertThat(closeVersionOfRecordsSql).isEqualToNormalizingNewlines("INSERT INTO datamart.adpTable_actual (id, pk2, col_1, col_2, sys_from, sys_op)\n" +
                "  SELECT DISTINCT ON (staging.id, staging.pk2) staging.id, staging.pk2, staging.col_1, staging.col_2, 10 AS sys_from, 0 AS sys_op \n" +
                "  FROM datamart.adpTable_staging staging\n" +
                "    LEFT JOIN datamart.adpTable_actual actual \n" +
                "    ON actual.id = staging.id AND actual.pk2 = staging.pk2 AND actual.sys_from = 10\n" +
                "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;");
    }

    @Test
    void shouldBeCorrectTruncateSql() {
        // act
        String closeVersionOfRecordsSql = sqlFactory.getTruncateSql(DATAMART, TABLE_NAME);

        // assert
        Assertions.assertThat(closeVersionOfRecordsSql).isEqualToNormalizingNewlines("TRUNCATE datamart.adpTable_staging;");
    }
}