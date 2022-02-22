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

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SingleDatasourceOperator extends SqlCall {
    private static final SqlOperator DISTRIBUTED_OP =
            new SqlSpecialOperator("DATASOURCE_TYPE", SqlKind.OTHER_DDL);
    private final SqlNode originalNode;
    private final SourceType value;

    public SingleDatasourceOperator(SqlParserPos pos, SqlNode originalNode) {
        super(pos);
        this.originalNode = originalNode;
        this.value = SqlNodeUtil.extractSourceType(originalNode);
    }

    @Override
    public SqlOperator getOperator() {
        return DISTRIBUTED_OP;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(originalNode);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (originalNode != null) {
            writer.keyword(this.getOperator().getName());
            SqlWriter.Frame frame = writer.startList("", "");
            writer.sep("=");
            originalNode.unparse(writer, 0, 0);
            writer.endList(frame);
        }
    }
}
