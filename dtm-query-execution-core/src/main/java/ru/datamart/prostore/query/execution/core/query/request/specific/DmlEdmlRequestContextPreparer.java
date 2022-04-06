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
package ru.datamart.prostore.query.execution.core.query.request.specific;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.dto.TableInfo;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.execution.core.base.configuration.AppConfiguration;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;

import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.core.query.utils.MetricsUtils.createRequestMetrics;

@Component
public class DmlEdmlRequestContextPreparer implements SpecificRequestContextPreparer {
    private static final int MIN_PRIORITY = 0;
    private final AppConfiguration coreConfiguration;
    private final EntityDao entityDao;

    public DmlEdmlRequestContextPreparer(AppConfiguration coreConfiguration, EntityDao entityDao) {
        this.coreConfiguration = coreConfiguration;
        this.entityDao = entityDao;
    }

    @Override
    public Future<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>> create(QueryRequest request, SqlNode node) {
        return isEdmlOperation(node, request)
                .map(isEdml -> {
                    if (isEdml) {
                        return new EdmlRequestContext(
                                createRequestMetrics(request),
                                new DatamartRequest(request),
                                node,
                                coreConfiguration.getEnvName());
                    }

                    return DmlRequestContext.builder()
                            .request(new DmlRequest(request))
                            .envName(coreConfiguration.getEnvName())
                            .metrics(createRequestMetrics(request))
                            .sourceType(getDmlSourceType(node))
                            .sqlNode(node)
                            .build();
                });
    }

    @Override
    public boolean isApplicable(SqlNode node) {
        return true;
    }

    @Override
    public int priority() {
        return MIN_PRIORITY;
    }

    private Future<Boolean> isEdmlOperation(SqlNode node, QueryRequest request) {
        return Future.future(promise -> {
            if (node.getKind() == SqlKind.ROLLBACK) {
                promise.complete(Boolean.TRUE);
                return;
            }

            if (node.getKind() != SqlKind.INSERT) {
                promise.complete(Boolean.FALSE);
                return;
            }

            hasAnyEdmlEntity((SqlInsert) node, request)
                    .onComplete(promise);
        });
    }

    private SourceType getDmlSourceType(SqlNode node) {
        if (node instanceof SqlDataSourceTypeGetter) {
            return ((SqlDataSourceTypeGetter) node).getDatasourceType().getValue();
        }

        return null;
    }

    protected Future<Boolean> hasAnyEdmlEntity(SqlInsert sqlInsert, QueryRequest queryRequest) {
        val defaultDatamartMnemonic = queryRequest.getDatamartMnemonic();
        val tableAndSnapshots = new SqlSelectTree(sqlInsert).findAllTableAndSnapshots();
        List<Future> entityFutures = tableAndSnapshots
                .stream()
                .map(item -> new TableInfo(item.tryGetSchemaName().orElse(defaultDatamartMnemonic),
                        item.tryGetTableName().orElseThrow(() -> cantDetermineTableException(queryRequest))))
                .distinct()
                .map(this::getEntityAndCheckType)
                .collect(Collectors.toList());
        return CompositeFuture.join(entityFutures)
                .map(CompositeFuture::<Boolean>list)
                .map(booleans -> booleans.stream().anyMatch(isEdml -> isEdml));
    }

    private Future<Boolean> getEntityAndCheckType(TableInfo tableInfo) {
        return entityDao.getEntity(tableInfo.getSchemaName(), tableInfo.getTableName())
                .map(entity -> entity.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE || entity.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE);
    }

    private DtmException cantDetermineTableException(QueryRequest queryRequest) {
        return new DtmException(String.format("Can't determine table from query [%s]", queryRequest.getSql()));
    }
}
