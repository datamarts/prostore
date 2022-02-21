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
package ru.datamart.prostore.query.calcite.core.extension.ddl;

import com.google.common.collect.ImmutableList;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@Getter
public class SqlDropMaterializedView extends SqlDrop implements SqlLogicalCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator(OperationNames.DROP_MATERIALIZED_VIEW, SqlKind.DROP_MATERIALIZED_VIEW);
    private final SingleDatasourceOperator destination;
    private final SqlIdentifier name;
    private final boolean isLogicalOnly;

    public SqlDropMaterializedView(SqlParserPos pos,
                                   boolean ifExists,
                                   SqlIdentifier name,
                                   SqlNode destination,
                                   boolean isLogicalOnly) {
        super(OPERATOR, pos, ifExists);
        this.name = name;
        this.destination = new SingleDatasourceOperator(pos, destination);
        this.isLogicalOnly = isLogicalOnly;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec) {
        writer.keyword(this.getOperator().getName());
        if (this.ifExists) {
            writer.keyword("IF EXISTS");
        }

        this.name.unparse(writer, leftPrec, rightPrec);
        this.destination.unparse(writer, leftPrec, rightPrec);

        if (this.isLogicalOnly) {
            writer.keyword("LOGICAL_ONLY");
        }
    }
}
