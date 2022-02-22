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
public class SqlAlterView extends SqlAlter {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator(OperationNames.ALTER_VIEW, SqlKind.ALTER_VIEW);
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;

    public SqlAlterView(SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList, SqlNode query) throws ParseException {
        super(pos, "VIEW");
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }

    @Override
    protected void unparseAlterOperation(SqlWriter writer, int i, int i1) {
        this.name.unparse(writer, i, i1);
        writer.keyword("AS");
        writer.keyword(" ");
        this.query.unparse(writer, 0, 0);
    }
}
