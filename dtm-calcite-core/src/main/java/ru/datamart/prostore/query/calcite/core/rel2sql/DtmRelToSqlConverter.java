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
package ru.datamart.prostore.query.calcite.core.rel2sql;

import ru.datamart.prostore.query.calcite.core.visitors.SqlDollarReplacementShuttle;
import lombok.val;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

public class DtmRelToSqlConverter {
    private final SqlDollarReplacementShuttle sqlDollarReplacementShuttle = new SqlDollarReplacementShuttle();
    private final SqlDialect sqlDialect;
    private final boolean allowStarInProject;

    public DtmRelToSqlConverter(SqlDialect sqlDialect, boolean allowStarInProject) {
        this.sqlDialect = sqlDialect;
        this.allowStarInProject = allowStarInProject;
    }

    public DtmRelToSqlConverter(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
        this.allowStarInProject = true;
    }

    public SqlNode convert(RelNode relNode) {
        return convert(relNode, allowStarInProject);
    }

    public SqlNode convertWithoutStar(RelNode relNode) {
        return convert(relNode, false);
    }

    public SqlNode convert(RelNode relNode, boolean allowStar) {
        val convertedNode = new NullNotCastableRelToSqlConverter(sqlDialect, allowStar)
                .visitChild(0, relNode)
                .asStatement();

        return convertedNode.accept(sqlDollarReplacementShuttle);
    }
}
