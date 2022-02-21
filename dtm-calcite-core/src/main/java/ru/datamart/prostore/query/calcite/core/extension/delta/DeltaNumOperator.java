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
package ru.datamart.prostore.query.calcite.core.extension.delta;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

public class DeltaNumOperator extends SqlCall {

    private final Long num;

    private static final SqlOperator OPERATOR_DELTA =
            new SqlSpecialOperator("SET", SqlKind.OTHER_DDL);

    public DeltaNumOperator(SqlParserPos pos, SqlNumericLiteral num) {
        super(pos);
        this.num = Optional.ofNullable(num).map(c -> c.longValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_DELTA;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (num != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(String.valueOf(this.num));
        }
    }

    public Long getNum() {
        return num;
    }
}
