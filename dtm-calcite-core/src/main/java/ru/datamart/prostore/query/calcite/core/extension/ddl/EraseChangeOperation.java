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

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;

import java.util.Collections;
import java.util.List;

public class EraseChangeOperation extends SqlDdl {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator(OperationNames.ERASE_CHANGE_OPERATION, SqlKind.OTHER_DDL);

    private final Long changeOperationNumber;
    private final String datamart;

    public EraseChangeOperation(SqlParserPos pos, SqlNode changeOperationNumber, SqlIdentifier datamart) {
        super(OPERATOR, pos);
        this.changeOperationNumber = ((SqlNumericLiteral) changeOperationNumber).longValue(true);
        this.datamart = datamart == null ? null : datamart.getSimple();
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(OPERATOR.getName() + "(" + changeOperationNumber);
        if (datamart != null) {
            writer.keyword(", " + datamart);
        }
        writer.keyword(")");
    }

    public String getDatamart() {
        return datamart;
    }

    public Long getChangeOperationNumber() {
        return changeOperationNumber;
    }
}
