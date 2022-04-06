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
package ru.datamart.prostore.query.calcite.core.extension.eddl.standalone;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateReadableExternalTable extends SqlCreateStandaloneExternalTable {

    private static final SqlOperator READABLE_EXTERNAL_TABLE_OPERATOR =
            new SqlSpecialOperator("CREATE READABLE EXTERNAL TABLE", SqlKind.OTHER_DDL);

    public SqlCreateReadableExternalTable(SqlParserPos pos,
                                          boolean ifNotExists,
                                          SqlIdentifier name,
                                          SqlNodeList columnList,
                                          SqlNodeList distributedBy,
                                          SqlNode location,
                                          SqlNode options) {
        super(READABLE_EXTERNAL_TABLE_OPERATOR, pos, ifNotExists, name, columnList, distributedBy, location, options);
    }
}
