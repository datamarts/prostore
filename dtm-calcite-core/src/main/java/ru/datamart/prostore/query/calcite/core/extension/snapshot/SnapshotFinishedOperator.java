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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SnapshotFinishedOperator extends SnapshotDeltaIntervalOperator {

    private static final SqlOperator FINISHED_IN_OPERATOR =
            new SqlSpecialOperator("FINISHED IN", SqlKind.OTHER_DDL);

    public SnapshotFinishedOperator(SqlParserPos pos, SqlNode period, SqlOperator operator) {
        super(pos, period, operator);
    }

    @Override
    public SqlOperator getOperator() {
        return FINISHED_IN_OPERATOR;
    }

}
