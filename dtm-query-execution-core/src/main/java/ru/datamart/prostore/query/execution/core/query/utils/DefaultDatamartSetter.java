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

import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;

import java.util.Arrays;

public final class DefaultDatamartSetter {
    private DefaultDatamartSetter() {
    }

    public static void mutateNode(SqlNode sqlNode, String datamart) {
        val selectTree = new SqlSelectTree(sqlNode);
        selectTree.findAllTableAndSnapshotWithChildren().forEach(n -> setDatamart(n, datamart));
    }

    private static void setDatamart(SqlTreeNode n, String defaultDatamart) {
        if (n.getNode() instanceof SqlSnapshot) {
            val snapshot = (SqlSnapshot) n.getNode();
            if (snapshot.getTableRef() instanceof SqlIdentifier) {
                val identifier = (SqlIdentifier) snapshot.getTableRef();
                setDatamartToIdentifier(identifier, defaultDatamart);
            }
        } else if (n.getNode() instanceof SqlIdentifier) {
            setDatamartToIdentifier(n.getNode(), defaultDatamart);
        }
    }

    private static void setDatamartToIdentifier(SqlIdentifier identifier, String defaultDatamart) {
        if (!identifier.isSimple()) {
            return;
        }

        val changedIdentifier = Arrays.asList(defaultDatamart, identifier.getSimple());
        identifier.setNames(changedIdentifier, null);
    }
}
