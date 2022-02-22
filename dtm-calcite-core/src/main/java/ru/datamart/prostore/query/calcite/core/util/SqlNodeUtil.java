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
package ru.datamart.prostore.query.calcite.core.util;

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.parser.ParseException;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.visitors.SqlAggregateFinder;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

import java.util.Set;
import java.util.stream.Collectors;

public final class SqlNodeUtil {
    private SqlNodeUtil() {
    }

    public static SqlNode copy(SqlNode sqlNode) {
        return new SqlSelectTree(sqlNode).copy().getRoot().getNode();
    }

    public static SqlNode checkViewQueryAndGet(SqlNode query) throws ParseException {
        if (query instanceof SqlSelect) {
            if (((SqlSelect) query).getFrom() == null) {
                throw new ParseException("View query must have from clause!");
            } else {
                return query;
            }
        } else if (query instanceof SqlOrderBy) {
            checkViewQueryAndGet(((SqlOrderBy) query).query);
            return query;
        } else {
            throw new ParseException(String.format("Type %s of query does not support!", query.getClass().getName()));
        }
    }

    public static Set<SourceType> extractSourceTypes(SqlNodeList sourceTypesSqlList) {
        if (sourceTypesSqlList == null) {
            return null;
        }

        return sourceTypesSqlList.getList().stream()
                .map(node -> SourceType.valueOfAvailable(node.toString()))
                .collect(Collectors.toSet());
    }

    public static SourceType extractSourceType(SqlNode destination) {
        if (destination == null) {
            return null;
        }

        return SourceType.valueOfAvailable(destination.toString().replace("'", ""));
    }

    public static boolean containsAggregates(SqlNode sqlNode) {
        val aggregateVisitor = new SqlAggregateFinder();
        sqlNode.accept(aggregateVisitor);
        return aggregateVisitor.isFoundAggregate();
    }
}
