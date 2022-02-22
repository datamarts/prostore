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

import ru.datamart.prostore.common.dto.TableInfo;
import ru.datamart.prostore.common.exception.DtmException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Optional;

/**
 * Utils for sqlNode
 */
public class SqlNodeUtils {

    private SqlNodeUtils() {
    }

    public static List<String> getTableNames(SqlCall sqlNode) {
        List<String> names = getOne(sqlNode, SqlIdentifier.class).names;
        if (CollectionUtils.isEmpty(names) || names.size() > 2) {
            throw new DtmException("Table name should be presented in the form" +
                    "[schema_name.table_name | table_name]");
        }
        return names;
    }

    public static TableInfo getTableInfo(SqlCall sqlNode, String defaultSchema) {
        List<String> tableNames = getTableNames(sqlNode);
        return new TableInfo(getSchema(tableNames, defaultSchema), getTableName(tableNames));
    }

    public static TableInfo getTableInfo(SqlIdentifier sqlNode, String defaultSchema) {
        List<String> tableNames = sqlNode.names;
        return new TableInfo(getSchema(tableNames, defaultSchema), getTableName(tableNames));
    }

    public static <T> T getOne(SqlCall sqlNode, Class<T> sqlNodeClass) {
        Optional<T> node = sqlNode.getOperandList().stream()
                .filter(sqlNodeClass::isInstance)
                .map(operand -> (T) operand)
                .findAny();
        if (node.isPresent()) {
            return node.get();
        }
        throw new DtmException("Could not find parameter in[" + sqlNode + "]");
    }

    private static String getTableName(List<String> names) {
        return names.get(names.size() - 1);
    }

    private static String getSchema(List<String> names, String defaultSchema) {
        return names.size() > 1 ? names.get(names.size() - 2) : defaultSchema;
    }
}
