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
package ru.datamart.prostore.calcite.adqm.extension.dml;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.List;

public class SqlFinalOperator extends SqlOperator {

    public static final String NAME = "FINAL";

    protected SqlFinalOperator() {
        super(NAME,
                SqlKind.FINAL,
                2,
                true,
                (SqlReturnTypeInference) null,
                (SqlOperandTypeInference) null,
                (SqlOperandTypeChecker) null);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    public SqlFinalTable createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode tableRef) {
        assert functionQualifier == null;
        return new SqlFinalTable(pos, tableRef);
    }

    public <R> void acceptCall(SqlVisitor<R> visitor, SqlCall call, boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler) {
        if (onlyExpressions) {
            List<SqlNode> operands = call.getOperandList();

            for (int i = 1; i < operands.size(); ++i) {
                argHandler.visitChild(visitor, call, i, (SqlNode) operands.get(i));
            }
        } else {
            super.acceptCall(visitor, call, false, argHandler);
        }
    }

    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlFinalTable finalTable = (SqlFinalTable) call;
        finalTable.getTableRef().unparse(writer, 0, 0);
        writer.keyword(NAME);
    }
}
