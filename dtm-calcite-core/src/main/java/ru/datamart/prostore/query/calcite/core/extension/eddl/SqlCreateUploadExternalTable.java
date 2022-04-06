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
package ru.datamart.prostore.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlCreateUploadExternalTable extends SqlCreate {
    private static final SqlOperator OPERATOR_TABLE =
            new SqlSpecialOperator("CREATE UPLOAD EXTERNAL TABLE", SqlKind.OTHER_DDL);

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final LocationOperator locationOperator;
    private final FormatOperator formatOperator;
    private final MessageLimitOperator messageLimitOperator;
    private final OptionsOperator options;

    public SqlCreateUploadExternalTable(SqlParserPos pos, boolean ifNotExists, SqlIdentifier name,
                                        SqlNodeList columnList, SqlNode location, SqlNode format, SqlNode chunkSize,
                                        SqlNode options) {
        super(OPERATOR_TABLE, pos, false, ifNotExists);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.locationOperator = new LocationOperator(pos, (SqlCharStringLiteral) location);
        this.formatOperator = new FormatOperator(pos, (SqlCharStringLiteral) format);
        this.messageLimitOperator = new MessageLimitOperator(pos, (SqlNumericLiteral) chunkSize);
        this.options = new OptionsOperator(pos, options);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name, this.columnList, this.locationOperator, this.formatOperator, this.messageLimitOperator, this.options);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        this.name.unparse(writer, leftPrec, rightPrec);
        if (this.columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : this.columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }

        this.locationOperator.unparse(writer, leftPrec, rightPrec);
        this.formatOperator.unparse(writer, leftPrec, rightPrec);
        this.messageLimitOperator.unparse(writer, leftPrec, rightPrec);
        this.options.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public LocationOperator getLocationOperator() {
        return locationOperator;
    }

    public FormatOperator getFormatOperator() {
        return formatOperator;
    }

    public MessageLimitOperator getMassageLimitOperator() {
        return messageLimitOperator;
    }

    public OptionsOperator getOptions() {
        return options;
    }
}
