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
package ru.datamart.prostore.query.calcite.core.extension.config.function;

import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigCall;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlConfigShow extends SqlConfigCall {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CONFIG_SHOW", SqlKind.OTHER_DDL);
    private final SqlCharStringLiteral parameterName;

    public SqlConfigShow(SqlParserPos pos, SqlNode parameterName) {
        super(pos);
        this.parameterName = (SqlCharStringLiteral) parameterName;
    }

    public SqlCharStringLiteral getParameterName() {
        return parameterName;
    }

    @Override
    public SqlConfigType getSqlConfigType() {
        return SqlConfigType.CONFIG_SHOW;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (parameterName != null) {
            writer.literal(OPERATOR + "('" + this.parameterName + "')");
            return;
        }

        writer.literal(OPERATOR + "()");
    }
}
