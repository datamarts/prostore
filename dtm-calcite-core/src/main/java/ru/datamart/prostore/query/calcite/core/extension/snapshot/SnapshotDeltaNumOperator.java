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

import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

@Getter
public class SnapshotDeltaNumOperator extends SqlCall {

    private final Long deltaNum;

    private static final SqlOperator DELTA_NUM_OPERATOR =
            new SqlSpecialOperator("DELTA_NUM", SqlKind.OTHER_DDL);
    private final SqlNumericLiteral deltaNumNode;

    public SnapshotDeltaNumOperator(SqlParserPos pos, SqlNumericLiteral deltaNumNode) {
        super(pos);
        this.deltaNumNode = deltaNumNode;
        this.deltaNum = Optional.ofNullable(this.deltaNumNode).map(c -> {
            if (c.isInteger()) {
                return c.longValue(true);
            } else {
                throw new IllegalArgumentException("DELTA_NUM is not integer value.");
            }
        }).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return DELTA_NUM_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (deltaNum != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(String.valueOf(this.deltaNum));
        }
    }

    public Long getDeltaNum() {
        return deltaNum;
    }
}
