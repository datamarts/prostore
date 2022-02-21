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
package ru.datamart.prostore.query.calcite.core.visitors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.List;
import java.util.stream.Collectors;

public class SqlDollarReplacementShuttle extends SqlShuttle {
    private static final String dollarReplacement = "__";

    @Override
    public SqlNode visit(SqlIdentifier id) {
        return replaceDollarIdentifier(id);
    }

    private SqlNode replaceDollarIdentifier(SqlIdentifier node) {
        if (node == null || node.getClass() != SqlIdentifier.class) {
            return node;
        }

        List<String> fixedNames = node.names.stream()
                .map(s -> s.replace("$", dollarReplacement))
                .collect(Collectors.toList());

        return new SqlIdentifier(fixedNames, node.getParserPosition());
    }
}
