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
package ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmColumnsCastService")
public class AdqmColumnsCastService extends AbstractColumnsCastService {
    private static final SqlNode INTEGER_TYPE = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO), SqlParserPos.ZERO);

    public AdqmColumnsCastService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        super(sqlDialect);
    }

    @Override
    protected SqlNode surroundWith(ColumnType logicalType, SqlTypeName sqlType, SqlNode nodeToSurround) {
        switch (logicalType) {
            case DATE:
                return surroundDateNode(nodeToSurround, sqlType);
            case TIME:
                return surroundTimeNode(nodeToSurround, sqlType);
            case TIMESTAMP:
                return surroundTimestampNode(nodeToSurround, sqlType);
            case BOOLEAN:
                return surroundBooleanNode(nodeToSurround);
            default:
                throw new IllegalArgumentException("Invalid type to surround");
        }
    }

    private SqlBasicCall surroundBooleanNode(SqlNode nodeToSurround) {
        return new SqlBasicCall(SqlStdOperatorTable.CAST, new SqlNode[]{nodeToSurround, INTEGER_TYPE}, nodeToSurround.getParserPosition());
    }

    @Override
    protected boolean isCastType(ColumnType logicalType) {
        switch (logicalType) {
            case TIMESTAMP:
            case TIME:
            case DATE:
            case BOOLEAN:
                return true;
            default:
                return false;
        }
    }
}
