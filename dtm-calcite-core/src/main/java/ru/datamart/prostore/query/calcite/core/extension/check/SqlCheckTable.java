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
package ru.datamart.prostore.query.calcite.core.extension.check;

import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.Objects;

public class SqlCheckTable extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_TABLE", SqlKind.CHECK);
    private final String table;
    private final String schema;

    public SqlCheckTable(SqlParserPos pos, SqlIdentifier id) {
        super(pos, id);
        String nameWithSchema = Objects.requireNonNull(id.toString());
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
    }

    public String getTable() {
        return table;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.TABLE;
    }
}
