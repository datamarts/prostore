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
package ru.datamart.prostore.query.calcite.core.extension.check;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public class SqlGetChanges extends SqlCheckCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("GET_CHANGES", SqlKind.CHECK);

    public SqlGetChanges(SqlParserPos pos, SqlIdentifier datamart) {
        super(pos, datamart);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(OPERATOR.getName());
        SqlWriter.Frame frame = writer.startList("(", ")");
        writer.sep(",");
        if (name != null) {
            name.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }

    @Override
    public String getSchema() {
        if (name == null) {
            return null;
        }

        return ((SqlIdentifier) name).getSimple();
    }

    @Override
    public CheckType getType() {
        return CheckType.CHANGES;
    }
}
