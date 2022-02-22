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
package ru.datamart.prostore.query.calcite.core.dialect;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class LimitSqlDialect extends PostgresqlSqlDialect {
    /**
     * Creates a LimitSqlDialect.
     *
     * @param context
     */
    public LimitSqlDialect(Context context) {
        super(context);
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
        Preconditions.checkArgument(fetch != null || offset != null);
        if (fetch != null) {
            final SqlWriter.Frame fetchFrame =
                    writer.startList(SqlWriter.FrameTypeEnum.FETCH);
            writer.newlineAndIndent();
            writer.keyword("LIMIT");
            fetch.unparse(writer, -1, -1);
            writer.endList(fetchFrame);
        }
        if (offset != null) {
            writer.newlineAndIndent();
            final SqlWriter.Frame offsetFrame =
                    writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
            writer.keyword("OFFSET");
            offset.unparse(writer, -1, -1);
            writer.endList(offsetFrame);
        }
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String charsetName, String val) {
        buf.append(literalQuoteString);
        buf.append(val.replace(literalEndQuoteString, literalEscapedQuote));
        buf.append(literalEndQuoteString);
    }
}
