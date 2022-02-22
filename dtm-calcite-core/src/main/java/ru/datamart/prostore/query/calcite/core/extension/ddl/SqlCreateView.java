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
package ru.datamart.prostore.query.calcite.core.extension.ddl;

import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.parser.ParseException;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

@Getter
public class SqlCreateView extends SqlCreate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator(OperationNames.CREATE_VIEW, SqlKind.CREATE_VIEW);
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;

    public SqlCreateView(SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query) throws ParseException {
        super(OPERATOR, pos, replace, false);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }

        writer.keyword("VIEW");
        this.name.unparse(writer, leftPrec, rightPrec);
        if (this.columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");

            for (SqlNode column : this.columnList) {
                writer.sep(",");
                column.unparse(writer, 0, 0);
            }

            writer.endList(frame);
        }

        writer.keyword("AS");
        writer.newlineAndIndent();
        this.query.unparse(writer, 0, 0);
    }
}
