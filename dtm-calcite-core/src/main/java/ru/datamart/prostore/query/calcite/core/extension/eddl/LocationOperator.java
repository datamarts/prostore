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
package ru.datamart.prostore.query.calcite.core.extension.eddl;

import ru.datamart.prostore.common.plugin.exload.Type;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class LocationOperator extends SqlCall {

    private static final SqlOperator OPERATOR_LOCATION =
            new SqlSpecialOperator("LOCATION", SqlKind.OTHER_DDL);
    private static final String DELIMITER = ":";
    private final Type type;
    private final String location;

    LocationOperator(SqlParserPos pos, SqlCharStringLiteral destinationInfo) {
        super(pos);

        String destination = destinationInfo.getNlsString().getValue();
        String[] strings = destination.split(DELIMITER);
        if (strings.length < 2) {
            throw new IllegalArgumentException("Data type not specified in string" + destinationInfo);
        }

        this.type = Type.findByName(strings[0]);
        this.location = destination;

    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_LOCATION;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        writer.literal("'" + this.location + "'");
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return super.toSqlString(dialect);
    }

    public Type getType() {
        return type;
    }

    public String getLocation() {
        return location;
    }
}
