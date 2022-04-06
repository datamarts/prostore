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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import ru.datamart.prostore.query.calcite.core.extension.ddl.DistributedOperator;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlDistributedByGetter;
import ru.datamart.prostore.query.calcite.core.extension.eddl.OptionsOperator;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

public abstract class SqlCreateStandaloneExternalTable extends SqlCreate implements SqlDistributedByGetter {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final DistributedOperator distributedBy;
    private final StandaloneLocationOperator location;
    private final OptionsOperator options;

    protected SqlCreateStandaloneExternalTable(SqlOperator operator,
                                               SqlParserPos pos,
                                               boolean ifNotExists,
                                               SqlIdentifier name,
                                               SqlNodeList columnList,
                                               SqlNodeList distributedBy,
                                               SqlNode location,
                                               SqlNode options) {
        super(operator, pos, false, ifNotExists);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.distributedBy = new DistributedOperator(pos, distributedBy);
        this.location = new StandaloneLocationOperator(pos, (SqlCharStringLiteral) location);
        this.options = new OptionsOperator(pos, options);
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name, this.columnList, this.distributedBy, this.location, this.options);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public DistributedOperator getDistributedBy() {
        return distributedBy;
    }

    public StandaloneLocationOperator getLocation() {
        return location;
    }

    public OptionsOperator getOptions() {
        return options;
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
        if (!distributedBy.isEmpty()) {
            this.distributedBy.unparse(writer, leftPrec, rightPrec);
        }
        this.location.unparse(writer, leftPrec, rightPrec);
        if (!options.isEmpty()) {
            this.options.unparse(writer, leftPrec, rightPrec);
        }
    }
}
