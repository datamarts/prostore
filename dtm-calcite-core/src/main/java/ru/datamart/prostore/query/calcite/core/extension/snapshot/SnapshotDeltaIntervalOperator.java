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
package ru.datamart.prostore.query.calcite.core.extension.snapshot;

import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.exception.DtmException;
import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SnapshotDeltaIntervalOperator extends SqlCall {

    private final SqlNode period;
    private final SelectOnInterval selectOnInterval;
    private final SqlOperator inOperator;

    private static final SqlOperator STARTED_IN_OPERATOR =
            new SqlSpecialOperator("", SqlKind.OTHER_DDL);

    public SnapshotDeltaIntervalOperator(SqlParserPos pos, SqlNode period, SqlOperator operator) {
        super(pos);
        this.period = period;
        this.inOperator = operator;
        this.selectOnInterval = createDeltaInterval();
    }

    @Override
    public SqlOperator getOperator() {
        return STARTED_IN_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.inOperator != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(this.selectOnInterval.getIntervalStr());
        }
    }

    public SelectOnInterval getDeltaInterval() {
        return selectOnInterval;
    }

    private SelectOnInterval createDeltaInterval() {
        if (this.inOperator != null) {
            SqlBasicCall periodCall = (SqlBasicCall) this.period;
            if (periodCall.getOperands().length == 0 || periodCall.getOperands().length > 2) {
                throw new DtmException("Delta interval must have two values!");
            }
            Long deltaFrom = Long.valueOf(String.valueOf(periodCall.getOperands()[0]));
            Long deltaTo = Long.valueOf(String.valueOf(periodCall.getOperands()[1]));
            if (deltaTo < deltaFrom) {
                throw new DtmException("Incorrect delta interval, deltaTo must be more than deltaFrom!");
            }
            return new SelectOnInterval(deltaFrom, deltaTo);
        } else {
            return null;
        }
    }
}
