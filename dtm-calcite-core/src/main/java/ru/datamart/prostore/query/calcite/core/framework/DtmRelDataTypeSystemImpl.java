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
package ru.datamart.prostore.query.calcite.core.framework;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class DtmRelDataTypeSystemImpl extends RelDataTypeSystemImpl {

    public DtmRelDataTypeSystemImpl() {
        super();
    }

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        switch(typeName) {
            case DECIMAL:
                return this.getMaxNumericPrecision();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return 2;
            case CHAR:
            case BINARY:
                return 1;
            case VARCHAR:
            case VARBINARY:
                return -1;
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case REAL:
                return 7;
            case FLOAT:
            case DOUBLE:
                return 15;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 6;
            case DATE:
                return 0;
            default:
                return -1;
        }
    }

    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        switch(typeName) {
            case DECIMAL:
                return this.getMaxNumericPrecision();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return 10;
            case CHAR:
            case VARCHAR:
                return 65536;
            case BINARY:
            case VARBINARY:
                return 65536;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 6;
            default:
                return this.getDefaultPrecision(typeName);
        }
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }
}
