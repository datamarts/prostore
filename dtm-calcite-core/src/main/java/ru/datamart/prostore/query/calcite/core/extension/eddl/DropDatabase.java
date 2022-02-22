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

import com.google.common.collect.ImmutableList;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlLogicalCall;
import lombok.Getter;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@Getter
public class DropDatabase extends SqlDrop implements SqlLogicalCall {
    private final SqlIdentifier name;
    private final boolean isLogicalOnly;

    private static final SqlOperator OPERATOR_DATABASE =
            new SqlSpecialOperator(OperationNames.DROP_DATABASE, SqlKind.DROP_SCHEMA);

    public DropDatabase(SqlParserPos pos,
                        boolean ifExists,
                        SqlIdentifier name,
                        boolean isLogicalOnly) {
        super(OPERATOR_DATABASE, pos, ifExists);
        this.name = name;
        this.isLogicalOnly = isLogicalOnly;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }
}
