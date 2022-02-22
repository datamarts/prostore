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
package ru.datamart.prostore.query.calcite.core.extension.check;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCheckData extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_DATA", SqlKind.CHECK);
    private final String table;
    private final String schema;
    private final Long deltaNum;
    private final Long normalization;
    private final Set<String> columns;

    public SqlCheckData(SqlParserPos pos, SqlIdentifier name, SqlLiteral deltaNum, SqlLiteral normalization, List<SqlNode> columns) {
        super(pos, name);
        final String nameWithSchema = name.toString();
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.table = CalciteUtil.parseTableName(nameWithSchema);
        this.deltaNum = deltaNum.longValue(true);
        this.normalization = normalization == null ? 1L : normalization.longValue(true);
        if (this.normalization < 1) {
            throw new DtmException("Normalization parameter must be greater than or equal to 1");
        }
        this.columns = columns == null ? null : columns.stream()
                .map(c -> ((SqlIdentifier) c).getSimple())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Long getDeltaNum() {
        return deltaNum;
    }

    public Long getNormalization() {
        return normalization;
    }

    public Set<String> getColumns() {
        return columns;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String delimiter = ", ";
        String delta = this.deltaNum.toString();
        writer.literal(OPERATOR + "(" + getTableName() + delimiter + delta + getTableColumns(delimiter) + ")");
    }

    private String getTableColumns(String delimiter) {
        if (this.columns == null) {
            return "";
        } else {
            return delimiter + "[" + String.join(delimiter, this.columns) + "]";
        }

    }

    private String getTableName() {
        return this.name.toString();
    }
}
