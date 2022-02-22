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
package ru.datamart.prostore.query.calcite.core.extension.config.function;

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigCall;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

public class SqlConfigStorageAdd extends SqlConfigCall {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CONFIG_STORAGE_ADD", SqlKind.OTHER_DDL);
    private final SourceType sourceType;

    public SqlConfigStorageAdd(SqlParserPos pos, SqlNode deltaDateTimeStr) {
        super(pos);
        this.sourceType = parseSourceType(Objects.requireNonNull((SqlCharStringLiteral) deltaDateTimeStr));
    }

    private SourceType parseSourceType(SqlCharStringLiteral stringLiteral) {
        String sourceTypeStr = stringLiteral.getNlsString().getValue().toUpperCase();
        return SourceType.valueOfAvailable(sourceTypeStr);
    }

    @Override
    public SqlConfigType getSqlConfigType() {
        return SqlConfigType.CONFIG_STORAGE_ADD;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.literal(
            OPERATOR + "(" + "'" +
                this.sourceType +
                "'" + ")");
    }

    public SourceType getSourceType() {
        return sourceType;
    }
}
