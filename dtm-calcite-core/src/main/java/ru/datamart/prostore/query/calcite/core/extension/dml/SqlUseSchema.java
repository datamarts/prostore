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
package ru.datamart.prostore.query.calcite.core.extension.dml;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlUseSchema extends SqlCall {

    private final SqlIdentifier datamart;
    private static final SqlOperator USE_OPERATOR = new SqlSpecialOperator("USE", SqlKind.OTHER);

    public SqlUseSchema(SqlParserPos pos, SqlIdentifier datamart) {
        super(pos);
        this.datamart = Objects.requireNonNull(datamart);
    }

    @Override
    public SqlOperator getOperator() {
        return USE_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(datamart);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        datamart.unparse(writer, leftPrec, rightPrec);
    }
}
