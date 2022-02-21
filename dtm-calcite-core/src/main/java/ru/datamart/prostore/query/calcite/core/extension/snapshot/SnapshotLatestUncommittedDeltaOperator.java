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
package ru.datamart.prostore.query.calcite.core.extension.snapshot;

import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

@Getter
public class SnapshotLatestUncommittedDeltaOperator extends SqlCall {

    private static final SqlOperator LATEST_UNCOMMITTED_DELTA_OPERATOR =
            new SqlSpecialOperator("LATEST_UNCOMMITTED_DELTA", SqlKind.OTHER_DDL);
    private final Boolean isLatestUncommittedDelta;
    private final SqlLiteral isLatestNode;

    public SnapshotLatestUncommittedDeltaOperator(SqlParserPos pos, SqlLiteral isLatest) {
        super(pos);
        isLatestNode = isLatest;
        this.isLatestUncommittedDelta = isLatest != null && isLatestNode.booleanValue();
    }

    @Override
    public SqlOperator getOperator() {
        return LATEST_UNCOMMITTED_DELTA_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (isLatestUncommittedDelta) {
            writer.keyword(this.getOperator().getName());
        }
    }

    public Boolean getIsLatestUncommittedDelta() {
        return isLatestUncommittedDelta;
    }
}
