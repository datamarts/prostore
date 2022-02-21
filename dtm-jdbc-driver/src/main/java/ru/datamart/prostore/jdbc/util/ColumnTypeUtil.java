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
package ru.datamart.prostore.jdbc.util;

import ru.datamart.prostore.common.model.ddl.ColumnType;

import static java.sql.Types.*;

public class ColumnTypeUtil {

    public static ColumnType getColumnType(int sqlType) throws DtmSqlException {
        switch (sqlType) {
            case SMALLINT:
            case TINYINT:
            case INTEGER:
                return ColumnType.INT;
            case BIGINT:
                return ColumnType.BIGINT;
            case CHAR:
                return ColumnType.CHAR;
            case VARCHAR:
                return ColumnType.VARCHAR;
            case FLOAT:
                return ColumnType.FLOAT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.TIME;
            case TIMESTAMP:
                return ColumnType.TIMESTAMP;
            case OTHER:
                return ColumnType.ANY;
            default:
                throw new DtmSqlException(String.format("Unknown mapping from sql type [%d]", sqlType));
        }
    }
}
