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

import com.google.common.collect.ImmutableList;
import lombok.val;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class SqlCheckCall extends SqlCall {
    protected final SqlNode name;

    protected SqlCheckCall(SqlParserPos pos, SqlNode name) {
        super(pos);
        this.name = name;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        val nodeName = getNodeName();
        writer.literal(this.getOperator() + "(" + nodeName + ")");
    }

    private String getNodeName() {
        return this.name == null ? "" : "'" + this.name + "'";
    }

    @Nonnull
    @Override
    public abstract SqlOperator getOperator();

    public abstract CheckType getType();

    public abstract String getSchema();
}
