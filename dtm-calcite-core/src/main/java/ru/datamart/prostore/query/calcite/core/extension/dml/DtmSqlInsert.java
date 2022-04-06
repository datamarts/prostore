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
package ru.datamart.prostore.query.calcite.core.extension.dml;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DtmSqlInsert extends SqlInsert implements SqlRetryable {
    private final boolean retry;

    public DtmSqlInsert(SqlParserPos pos, SqlNodeList keywords, SqlNode targetTable, SqlNode source, SqlNodeList columnList, boolean retry) {
        super(pos, keywords, targetTable, source, columnList);
        this.retry = retry;
    }

    @Override
    public boolean isRetry() {
        return retry;
    }
}
