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
package ru.datamart.prostore.query.calcite.core.extension.delta.function;

import ru.datamart.prostore.query.calcite.core.extension.delta.SqlDeltaCall;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SqlGetDeltaByDateTime extends SqlDeltaCall {

    private String deltaDateTime;
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("GET_DELTA_BY_DATETIME", SqlKind.OTHER_DDL);

    public SqlGetDeltaByDateTime(SqlParserPos pos, SqlNode deltaDateTimeStr) {
        super(pos);
        this.deltaDateTime = getDeltaDateTime(Objects.requireNonNull((SqlCharStringLiteral) deltaDateTimeStr));
    }

    private String getDeltaDateTime(SqlCharStringLiteral deltaDateTimeStr) {
        return deltaDateTimeStr.getNlsString().getValue();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(
                OPERATOR + "(" + "'" +
                        this.deltaDateTime +
                        "'" + ")");
    }

    public String getDeltaDateTime() {
        return deltaDateTime;
    }
}
