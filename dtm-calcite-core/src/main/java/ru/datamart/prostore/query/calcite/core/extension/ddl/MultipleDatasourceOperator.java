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
import java.util.Set;

@Getter
public class MultipleDatasourceOperator extends SqlCall {
    private static final SqlOperator DATASOURCE_TYPE_OP =
            new SqlSpecialOperator("DATASOURCE_TYPE", SqlKind.OTHER_DDL);
    private final SqlNodeList datasourceTypesNode;
    private final Set<SourceType> datasourceTypes;

    public MultipleDatasourceOperator(SqlParserPos pos, SqlNodeList datasourceTypesNode) {
        super(pos);
        this.datasourceTypesNode = datasourceTypesNode;
        this.datasourceTypes = SqlNodeUtil.extractSourceTypes(datasourceTypesNode);
    }

    @Override
    public SqlOperator getOperator() {
        return DATASOURCE_TYPE_OP;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(datasourceTypesNode);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (datasourceTypesNode != null) {
            writer.keyword(this.getOperator().getName());
            SqlWriter.Frame frame = writer.startList("(", ")");
            datasourceTypesNode.unparse(writer, 0, 0);
            writer.endList(frame);
        }
    }
}
