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
package ru.datamart.prostore.query.calcite.core.extension.delta;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

public class ResumeWriteOperation extends SqlDeltaCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("RESUME_WRITE_OPERATION", SqlKind.OTHER_DDL);

    private final Long writeOperationNumber;

    public ResumeWriteOperation(SqlParserPos pos, SqlNode writeOperationNumber) {
        super(pos);
        this.writeOperationNumber = writeOperationNumber == null ? null : ((SqlNumericLiteral) writeOperationNumber).longValue(true);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(OPERATOR.getName() + "(");
        if (writeOperationNumber != null) {
            writer.keyword(String.valueOf(writeOperationNumber));
        }
        writer.keyword(")");
    }

    public Long getWriteOperationNumber() {
        return writeOperationNumber;
    }
}
