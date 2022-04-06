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
package ru.datamart.prostore.query.execution.plugin.adg.dml.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import lombok.val;

import java.util.List;
import java.util.stream.Collectors;

public class AdgDmlSqlFactory {

    private static final String INSERT_INTO_TEMPLATE = "INSERT INTO \"%s__%s__%s_staging\" (%s,\"sys_op\") %s";
    private static final String INSERT_INTO_STANDALONE = "INSERT INTO \"%s\" (%s) %s";

    private AdgDmlSqlFactory() {
    }

    public static String createDeleteSql(String datamart, String env, Entity entity, String enrichedSelect) {
        val columns = EntityFieldUtils.getNotNullableFields(entity).stream()
                .map(EntityField::getName)
                .map(s -> String.format("\"%s\"", s))
                .collect(Collectors.joining(","));
        return String.format(INSERT_INTO_TEMPLATE, env, datamart, entity.getName(), columns, enrichedSelect);
    }

    public static String createInsertSelectSql(String datamart, String env, String entityName, List<EntityField> columns, String enrichedSelect) {
        val concatColumns = getColumns(columns);
        return String.format(INSERT_INTO_TEMPLATE, env, datamart, entityName, concatColumns, enrichedSelect);
    }

    public static String createStandaloneInsertSelectSql(String entityPath, List<EntityField> columns, String enrichedSelect) {
        val concatColumns = getColumns(columns);
        return String.format(INSERT_INTO_STANDALONE, entityPath, concatColumns, enrichedSelect);
    }

    private static String getColumns(List<EntityField> columns) {
        return columns.stream()
                .map(EntityField::getName)
                .map(s -> String.format("\"%s\"", s))
                .collect(Collectors.joining(","));
    }
}
