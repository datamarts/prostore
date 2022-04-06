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

import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
@ToString
public class EraseWriteOperation extends SqlDeltaCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("ERASE_WRITE_OPERATION", SqlKind.OTHER_DDL);

    private final Long writeOperationNumber;
    private final String datamart;

    public EraseWriteOperation(SqlParserPos pos, SqlNode writeOperationNumber, SqlIdentifier datamart) {
        super(pos);
        this.writeOperationNumber = ((SqlNumericLiteral) writeOperationNumber).longValue(true);
        this.datamart = datamart == null ? null : datamart.getSimple();
    }

}
