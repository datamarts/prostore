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
package ru.datamart.prostore.query.calcite.core.extension.check;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCheckSumSnapshot extends SqlCheckCall implements SqlCheckSum {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_SUM_SNAPSHOT", SqlKind.CHECK);
    private final Long deltaNum;
    private final Long normalization;
    private final String schema;
    private final String table;
    private final Set<String> columns;

    public SqlCheckSumSnapshot(SqlParserPos pos, SqlLiteral deltaNum, SqlLiteral normalization, SqlIdentifier table, List<SqlNode> columns) {
        super(pos, table);
        this.deltaNum = deltaNum.longValue(true);
        this.normalization = normalization == null ? 1L : normalization.longValue(true);
        if (this.normalization < 1) {
            throw new DtmException("Normalization parameter must be greater than or equal to 1");
        }
        this.schema = table == null ? null : CalciteUtil.parseSchemaName(table.toString());
        this.table = table == null ? null : CalciteUtil.parseTableName(table.toString());
        this.columns = columns == null ? null : columns.stream()
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.SUM;
    }

    @Override
    public CheckSumType getChecksumType() {
        return CheckSumType.SNAPSHOT;
    }

    @Override
    public Long getDeltaNum() {
        return deltaNum;
    }

    @Override
    public Long getNormalization() {
        return normalization;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public Set<String> getColumns() {
        return columns;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String delta = this.deltaNum.toString();
        writer.literal(OPERATOR + "(" + delta + getTableWithColumns() + ")");
    }

    private String getTableWithColumns() {
        if (this.table == null) {
            return "";
        } else {
            final String delimiter = ", ";
            if (this.columns == null) {
                return delimiter + getTableName();
            } else {
                return delimiter + getTableName() + delimiter + getTableColumns();
            }
        }
    }

    private String getTableName() {
        return this.name.toString();
    }

    private String getTableColumns() {
        return "[" + String.join(", ", this.columns) + "]";
    }
}
