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
package ru.datamart.prostore.query.calcite.core.util;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public final class SqlNodeTemplates {

    private static final SqlLiteral NULL_LITERAL = SqlLiteral.createNull(SqlParserPos.ZERO);

    private SqlNodeTemplates() {
    }

    public static SqlBasicCall as(String origin, String alias) {
        return new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{new SqlIdentifier(origin, SqlParserPos.ZERO),
                new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
    }

    public static SqlBasicCall as(SqlNode origin, String alias) {
        return new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{origin, new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
    }

    public static SqlNodeList nodeList(Collection<? extends SqlNode> sqlNodes) {
        return new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
    }

    public static SqlIdentifier identifier(String... parts) {
        return new SqlIdentifier(Arrays.asList(parts), SqlParserPos.ZERO);
    }

    public static SqlLiteral longLiteral(long value) {
        return SqlLiteral.createExactNumeric(Long.toString(value), SqlParserPos.ZERO);
    }

    public static SqlBasicCall basicCall(SqlOperator sqlOperator, SqlNode left, SqlNode right) {
        return new SqlBasicCall(sqlOperator, new SqlNode[]{left, right}, SqlParserPos.ZERO);
    }

    public static SqlBasicCall basicCall(SqlOperator sqlOperator, SqlNode[] nodes) {
        return new SqlBasicCall(sqlOperator, nodes, SqlParserPos.ZERO);
    }

    public static SqlBasicCall basicCall(SqlOperator sqlOperator, List<SqlNode> nodes) {
        return new SqlBasicCall(sqlOperator, nodes.toArray(new SqlNode[0]), SqlParserPos.ZERO);
    }

    public static SqlLiteral literalForParameter(Object value, ColumnType columnType) {
        if (value == null) {
            return NULL_LITERAL;
        }
        switch (columnType) {
            case VARCHAR:
            case CHAR:
            case BLOB:
            case UUID:
            case LINK:
            case ANY:
                return SqlLiteral.createCharString(value.toString(), SqlParserPos.ZERO);
            case BIGINT:
            case INT:
            case INT32:
            case DOUBLE:
            case FLOAT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return SqlLiteral.createExactNumeric(value.toString(), SqlParserPos.ZERO);
            case BOOLEAN:
                return SqlLiteral.createBoolean((boolean) value, SqlParserPos.ZERO);
            default:
                throw new IllegalArgumentException("Unknown type [" + columnType + "] for literal creation");
        }
    }
}
