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
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SqlCreateTable extends SqlCreate implements SqlLogicalCall, SqlDistributedByGetter {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator(OperationNames.CREATE_TABLE, SqlKind.CREATE_TABLE);
    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private final DistributedOperator distributedBy;
    private final MultipleDatasourceOperator destination;
    private final boolean isLogicalOnly;

    public SqlCreateTable(SqlParserPos pos,
                          boolean isLogicalOnly,
                          boolean ifNotExists,
                          SqlIdentifier name,
                          SqlNodeList columnList,
                          SqlNode query,
                          SqlNodeList distributedBy,
                          SqlNodeList destination) {
        super(OPERATOR, pos, false, ifNotExists);
        this.name = name;
        this.columnList = columnList;
        this.query = query;
        this.distributedBy = new DistributedOperator(pos, distributedBy);
        this.destination = new MultipleDatasourceOperator(pos, destination);
        this.isLogicalOnly = isLogicalOnly;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, query, distributedBy, destination);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }

        distributedBy.unparse(writer, 0, 0);
        destination.unparse(writer, 0, 0);

        if (query != null) {
            writer.keyword("AS");
            writer.newlineAndIndent();
            query.unparse(writer, 0, 0);
        }

        if (isLogicalOnly) {
            writer.keyword("LOGICAL_ONLY");
        }
    }
}
