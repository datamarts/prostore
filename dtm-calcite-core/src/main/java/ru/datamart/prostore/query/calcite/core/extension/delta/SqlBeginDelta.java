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
package ru.datamart.prostore.query.calcite.core.extension.delta;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlBeginDelta extends SqlDeltaCall {

    private DeltaNumOperator deltaNumOperator;
    private static final SqlOperator BEGIN_DELTA_OPERATOR =
            new SqlSpecialOperator("BEGIN DELTA", SqlKind.OTHER_DDL);

    public SqlBeginDelta(SqlParserPos pos, SqlNode num) {
        super(pos);
        this.deltaNumOperator = new DeltaNumOperator(pos, (SqlNumericLiteral) num);
    }

    @Override
    public SqlOperator getOperator() {
        return BEGIN_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(deltaNumOperator);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        deltaNumOperator.unparse(writer, leftPrec, rightPrec);
    }

    public DeltaNumOperator getDeltaNumOperator() {
        return deltaNumOperator;
    }
}
