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
package ru.datamart.prostore.query.execution.core.query.utils;

import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class DefaultDatamartSetter {

    public SqlNode set(SqlNode sqlNode, String datamart) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        selectTree.findAllTableAndSnapshotWithChildren().forEach(n -> setDatamart(n, datamart));
        return sqlNode;
    }

    private void setDatamart(SqlTreeNode n, String defaultDatamart) {
        if (n.getNode() instanceof SqlSnapshot) {
            SqlSnapshot snapshot = n.getNode();
            if (snapshot.getTableRef() instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) snapshot.getTableRef();
                snapshot.setOperand(0, getSqlIdentifier(defaultDatamart, identifier));
            }
        } else if (n.getNode() instanceof SqlIdentifier) {
            setDatamartToIdentifier(n, defaultDatamart);
        }
    }

    private void setDatamartToIdentifier(SqlTreeNode n, String defaultDatamart) {
        SqlIdentifier identifier = getSqlIdentifier(defaultDatamart, n.getNode());
        ((SqlIdentifier) n.getNode()).assignNamesFrom(identifier);
    }

    private SqlIdentifier getSqlIdentifier(String defaultDatamart, SqlIdentifier node) {
        SqlIdentifier identifier = node;
        if (identifier.isSimple()) {
            identifier = new SqlIdentifier(Arrays.asList(defaultDatamart, identifier.getSimple()),
                    identifier.getParserPosition());
        }
        return identifier;
    }
}
