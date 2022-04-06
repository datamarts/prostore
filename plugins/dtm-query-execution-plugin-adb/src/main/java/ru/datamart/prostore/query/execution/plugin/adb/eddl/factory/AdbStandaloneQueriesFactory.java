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
package ru.datamart.prostore.query.execution.plugin.adb.eddl.factory;

import lombok.val;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil;

import java.util.Comparator;
import java.util.stream.Collectors;

public class AdbStandaloneQueriesFactory {
    private static final String CREATE_STANDALONE_PATTERN = "CREATE TABLE %s \n" +
            "(%s,\n PRIMARY KEY (%s)) \n DISTRIBUTED BY (%s)";
    private static final String DROP_STANDALONE_PATTERN = "DROP TABLE %s";

    private AdbStandaloneQueriesFactory() {
    }

    public static String createQuery(Entity entity) {
        val columns = entity.getFields();
        val pkFields = EntityFieldUtils.getPrimaryKeyList(columns).stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        val shardingKeys = EntityFieldUtils.getShardingKeyList(columns).stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(", "));
        val columnList = columns.stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> String.format("%s %s %s", field.getName(),
                        AdbTypeUtil.adbTypeFromDtmType(field),
                        field.getNullable() ? "" : "NOT NULL"))
                .collect(Collectors.joining(", "));
        return String.format(CREATE_STANDALONE_PATTERN, entity.getExternalTableLocationPath(),
                columnList, pkFields, shardingKeys);
    }

    public static String dropQuery(String externalTablePath) {
        return String.format(DROP_STANDALONE_PATTERN, externalTablePath);
    }
}
