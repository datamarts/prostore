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
import org.apache.calcite.util.ImmutableNullableList;
import ru.datamart.prostore.common.reader.SourceType;

import javax.annotation.Nonnull;
import java.util.List;

public class StandaloneLocationOperator extends SqlCall {

    private static final SqlOperator STANDALONE_LOCATION_OPERATOR =
            new SqlSpecialOperator("LOCATION", SqlKind.OTHER_DDL);
    private static final String DELIMITER = "://";
    private final SourceType type;
    private final String path;

    StandaloneLocationOperator(SqlParserPos pos, SqlCharStringLiteral locationInfo) {
        super(pos);

        String destination = locationInfo.getNlsString().getValue();
        String[] strings = destination.split(DELIMITER);
        if (strings.length < 2) {
            throw new IllegalArgumentException("Data type not specified in string" + locationInfo);
        }

        this.type = SourceType.valueOfAvailable(strings[0].substring(5));
        this.path = strings[1];

    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return STANDALONE_LOCATION_OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        writer.literal("'core:" + type.name().toLowerCase() + DELIMITER + this.path + "'");
    }

    public SourceType getType() {
        return type;
    }

    public String getPath() {
        return path;
    }
}
