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
import ru.datamart.prostore.query.calcite.core.extension.eddl.OptionsOperator;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

public abstract class SqlDropStandaloneExternalTable extends SqlDrop {

    private final SqlIdentifier name;
    private final OptionsOperator options;

    protected SqlDropStandaloneExternalTable(SqlOperator operator,
                                             SqlParserPos pos,
                                             boolean ifExists,
                                             SqlIdentifier name,
                                             SqlNode options) {
        super(operator, pos, ifExists);
        this.name = Objects.requireNonNull(name);
        this.options = new OptionsOperator(pos, options);
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name, this.options);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public OptionsOperator getOptions() {
        return options;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        if (!options.isEmpty()) {
            this.options.unparse(writer, leftPrec, rightPrec);
        }
    }
}
