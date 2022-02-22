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

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

public class ChunkSizeOperator extends SqlCall {

    private static final SqlOperator OPERATOR_CHUNK_SIZE =
            new SqlSpecialOperator("CHUNK_SIZE", SqlKind.OTHER_DDL);
    private final Integer chunkSize;

    public ChunkSizeOperator(SqlParserPos pos, SqlNumericLiteral chunkSize) {
        super(pos);
        this.chunkSize = Optional.ofNullable(chunkSize).map(c -> c.intValue(true)).orElse(null);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR_CHUNK_SIZE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    public Integer getChunkSize() {
        return chunkSize;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (chunkSize != null) {
            writer.keyword(this.getOperator().getName());
            writer.keyword(String.valueOf(this.chunkSize));
        }
    }
}
