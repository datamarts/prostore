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
package ru.datamart.prostore.query.execution.core.base.utils;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import org.apache.calcite.sql.type.SqlTypeName;

import static ru.datamart.prostore.common.model.ddl.ColumnType.*;

public class ColumnTypeUtil {
    public static ColumnType valueOf(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case VARCHAR:
                return VARCHAR;
            case CHAR:
                return CHAR;
            case BIGINT:
                return ColumnType.BIGINT;
            case INTEGER:
                return ColumnType.INT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case FLOAT:
                return ColumnType.FLOAT;
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.TIME;
            case TIMESTAMP:
                return ColumnType.TIMESTAMP;
            default:
                return ColumnType.ANY;
        }
    }

    public static ColumnType fromTypeString(String typeString) {
        switch (typeString) {
            case "varchar":
                return VARCHAR;
            case "char":
                return CHAR;
            case "bigint":
                return BIGINT;
            case "int":
            case "integer":
                return INT;
            case "int32":
                return INT32;
            case "link":
                return LINK;
            case "double":
                return DOUBLE;
            case "float":
                return FLOAT;
            case "date":
                return DATE;
            case "time":
                return TIME;
            case "timestamp":
                return TIMESTAMP;
            case "boolean":
                return BOOLEAN;
            case "blob":
                return BLOB;
            case "uuid":
                return UUID;
            case "any":
            default:
                return ANY;
        }
    }
}
