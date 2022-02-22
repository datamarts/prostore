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
package ru.datamart.prostore.query.calcite.core.extension.eddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

public class MessageLimitOperator extends SqlCall {

    private final Integer messageLimit;

    private static final SqlOperator OPERATOR_MESSAGE_LIMIT =
            new SqlSpecialOperator("MESSAGE_LIMIT", SqlKind.OTHER_DDL);

    public MessageLimitOperator(SqlParserPos pos, SqlNumericLiteral messageLimit) {
        super(pos);
        this.messageLimit = Optional.ofNullable(messageLimit).map(c -> c.intValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_MESSAGE_LIMIT;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    public Integer getMessageLimit() {
        return messageLimit;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (messageLimit != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(String.valueOf(this.messageLimit));
        }
    }
}
