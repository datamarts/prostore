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
package ru.datamart.prostore.query.calcite.core.extension.ddl.truncate;

import ru.datamart.prostore.common.ddl.TruncateType;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlTruncateHistory extends SqlCall implements SqlBaseTruncate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator(OperationNames.TRUNCATE_HISTORY, SqlKind.OTHER_DDL);
    private static final String INFINITE = "infinite";
    private static final int NAME_OPERAND_IDX = 0;
    private final List<SqlNode> operandList;
    private final String table;
    private final LocalDateTime datetime;
    private final boolean isInfinite;
    private final SqlNode conditions;

    public SqlTruncateHistory(SqlParserPos pos, SqlIdentifier name, SqlNode datetime, SqlNode conditions) {
        super(pos);
        String nameWithSchema = Objects.requireNonNull(name.toString());
        this.operandList = new ArrayList<>();
        operandList.add(name);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
        String datetimeStr = ((SqlCharStringLiteral) datetime).getNlsString().getValue();
        if (INFINITE.equalsIgnoreCase(datetimeStr)) {
            this.datetime = null;
            this.isInfinite = true;
        } else {
            this.datetime = CalciteUtil.parseLocalDateTime(datetimeStr);
            this.isInfinite = false;
        }
        this.conditions = conditions;
    }

    public TruncateType getTruncateType() {
        return TruncateType.HISTORY;
    }

    private SqlIdentifier getName() {
        return (SqlIdentifier) operandList.get(NAME_OPERAND_IDX);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return operandList;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        operandList.set(i, operand);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(String.format("%s %s", this.getOperator(), this.getName()));
    }

    public String getTable() {
        return table;
    }

    public LocalDateTime getDateTime() {
        return datetime;
    }

    public boolean isInfinite() {
        return isInfinite;
    }

    public SqlNode getConditions() {
        return conditions;
    }
}
