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
package ru.datamart.prostore.query.execution.core.dml.service.view;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.extension.snapshot.SqlDeltaSnapshot;
import ru.datamart.prostore.query.calcite.core.node.SqlKindKey;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ViewReplacerService {
    private static final SqlKindKey AS_CHECK = new SqlKindKey(SqlKind.AS, null);
    private final EntityDao entityDao;
    private final ViewReplacer logicViewReplacer;
    private final ViewReplacer materializedViewReplacer;

    public ViewReplacerService(EntityDao entityDao,
                               @Qualifier("logicViewReplacer") ViewReplacer logicViewReplacer,
                               @Qualifier("materializedViewReplacer") ViewReplacer materializedViewReplacer) {
        this.entityDao = entityDao;
        this.logicViewReplacer = logicViewReplacer;
        this.materializedViewReplacer = materializedViewReplacer;
    }

    public Future<SqlNode> replace(SqlNode sql, String datamart) {
        return Future.future((Promise<SqlNode> promise) -> {
            log.debug("before replacing:\n{}", sql);
            val rootSqlNode = SqlNodeUtil.copy(sql);

            val replaceContext = ViewReplaceContext.builder()
                    .viewReplacerService(this)
                    .viewQueryNode(rootSqlNode)
                    .datamart(datamart)
                    .build();

            replace(replaceContext).onSuccess(result -> {
                log.debug("after replacing: [{}]", rootSqlNode);
                promise.complete(rootSqlNode);
            }).onFailure(promise::fail);
        });
    }

    protected Future<Void> replace(ViewReplaceContext parentContext) {
        val allNodes = new SqlSelectTree(parentContext.getViewQueryNode());
        val allTableAndSnapshots = allNodes.findAllTableAndSnapshots();

        return CompositeFuture.join(allTableAndSnapshots
                .stream()
                .map(currentNode ->
                        extractEntity(currentNode, parentContext.getDatamart())
                                .compose(entity -> processEntity(parentContext,
                                        entity,
                                        currentNode,
                                        allNodes))
                )
                .collect(Collectors.toList()))
                .onSuccess(cf -> expandView(parentContext, allNodes))
                .mapEmpty();
    }

    private Future<Entity> extractEntity(SqlTreeNode node, String datamart) {
        return entityDao.getEntity(
                node.tryGetSchemaName().orElse(datamart),
                node.tryGetTableName().orElseThrow(() -> new RuntimeException("Can't get tableName for datamart " + datamart))
        );
    }

    private Future<Void> processEntity(ViewReplaceContext parentContext,
                                       Entity entity,
                                       SqlTreeNode currentNode,
                                       SqlSelectTree allNodes) {
        val currSnapshot = getSqlSnapshot(parentContext.getCurrentNode(), parentContext.getSqlSnapshot(), entity);
        val nodeContext = ViewReplaceContext.builder()
                .viewReplacerService(this)
                .entity(entity)
                .datamart(parentContext.getDatamart())
                .currentNode(currentNode)
                .sqlSnapshot(currSnapshot)
                .allNodes(allNodes)
                .build();

        return replaceView(parentContext, nodeContext);
    }

    private Future<Void> replaceView(ViewReplaceContext parentContext, ViewReplaceContext nodeContext) {
        switch (nodeContext.getEntity().getEntityType()) {
            case VIEW:
                return logicViewReplacer.replace(nodeContext);
            case MATERIALIZED_VIEW:
                return materializedViewReplacer.replace(nodeContext);
            default:
                return expandNode(parentContext, nodeContext);
        }
    }

    private Future<Void> expandNode(ViewReplaceContext parentContext, ViewReplaceContext nodeContext) {
        if (nodeContext.getSqlSnapshot() != null) {
            val parentSnapshot = (SqlDeltaSnapshot) parentContext.getCurrentNode().getNode();
            val childSnapshot = parentSnapshot.copy(nodeContext.getCurrentNode().getNode());
            nodeContext.getCurrentNode().getSqlNodeSetter().accept(childSnapshot);
        }
        return Future.succeededFuture();
    }

    private SqlSnapshot getSqlSnapshot(SqlTreeNode parentNode, SqlSnapshot sqlSnapshot, Entity entity) {
        if (parentNode == null || entity.getEntityType() == EntityType.READABLE_EXTERNAL_TABLE) {
            return null;
        }
        if (sqlSnapshot == null) {
            return parentNode.getNode() instanceof SqlSnapshot ? parentNode.getNode() : null;
        }
        return sqlSnapshot;
    }

    private void expandView(ViewReplaceContext context, SqlSelectTree nodes) {
        if (context.getAllNodes() != null && context.getEntity() != null) {
            if (isAliasExists(context.getAllNodes(), context.getCurrentNode())) {
                context.getCurrentNode().getSqlNodeSetter().accept(nodes.getRoot().getNode());
            } else {
                SqlBasicCall alias = getAlias(context.getCurrentNode(), nodes);
                context.getCurrentNode().getSqlNodeSetter().accept(alias);
            }
        }
    }

    private SqlBasicCall getAlias(SqlTreeNode parentNode, SqlSelectTree tree) {
        val tableName = parentNode.tryGetTableName()
                .orElseThrow(() -> new RuntimeException("Can't get tableName"));
        return new SqlBasicCall(new SqlAsOperator(),
                new SqlNode[]{tree.getRoot().getNode(),
                        new SqlIdentifier(tableName, SqlParserPos.QUOTED_ZERO)},
                SqlParserPos.QUOTED_ZERO);
    }

    private boolean isAliasExists(SqlSelectTree tree, SqlTreeNode node) {
        val parentNode = tree.getParentByChild(node);
        if (parentNode != null) {
            List<SqlKindKey> kindPath = parentNode.getKindPath();
            if (kindPath.isEmpty()) {
                return false;
            }
            return kindPath.get(kindPath.size() - 1).equals(AS_CHECK);
        }

        return false;
    }

}
