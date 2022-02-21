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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckSumType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckSumRequestContext;
import ru.datamart.prostore.query.execution.core.check.exception.CheckSumException;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CheckSumTableService {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;

    @Autowired
    public CheckSumTableService(DataSourcePluginService dataSourcePluginService, EntityDao entityDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
    }

    public Future<Long> calcCheckSumForAllTables(CheckSumRequestContext request) {
        return entityDao.getEntityNamesByDatamart(request.getDatamart())
                .compose(entityNames -> getEntities(entityNames, request.getDatamart()))
                .compose(entities -> calcCheckSumForEntities(entities, request))
                .map(checkSumList -> checkSumList.stream()
                        .reduce(0L, Long::sum));
    }

    public Future<Long> calcCheckSumTable(CheckSumRequestContext request) {
        val futures = request.getEntity().getDestination().stream()
                .map(sourceType -> checkSumInDatasource(sourceType, request))
                .collect(Collectors.<Future>toList());
        return CompositeFuture.join(futures)
                .map(result -> {
                    List<Pair<SourceType, Long>> resultList = result.list();
                    long distinctCount = resultList.stream()
                            .map(Pair::getValue)
                            .distinct().count();
                    if (distinctCount == 1) {
                        return resultList.get(0).getValue();
                    } else {
                        val pluginResults = resultList.stream()
                                .map(pair -> String.format("%s : %s", pair.getKey(), pair.getValue()))
                                .collect(Collectors.joining("\n"));
                        throw new CheckSumException(request.getEntity().getName(), pluginResults);
                    }
                });
    }

    private Future<Pair<SourceType, Long>> checkSumInDatasource(SourceType sourceType, CheckSumRequestContext request) {
        val checkDataRequest = new CheckDataByHashInt32Request(
                request.getCheckContext().getRequest().getQueryRequest().getRequestId(),
                request.getCheckContext().getEnvName(),
                request.getDatamart(),
                request.getEntity(),
                request.getCnFrom(),
                request.getCnTo(),
                getColumns(request),
                request.getNormalization());


        if (request.getChecksumType() == CheckSumType.SNAPSHOT) {
            return dataSourcePluginService.checkDataSnapshotByHashInt32(sourceType, request.getCheckContext().getMetrics(), checkDataRequest)
                    .map(result -> Pair.of(sourceType, result));
        }

        return dataSourcePluginService.checkDataByHashInt32(sourceType, request.getCheckContext().getMetrics(), checkDataRequest)
                .map(result -> Pair.of(sourceType, result));
    }

    private Set<String> getColumns(CheckSumRequestContext request) {
        return request.getColumns() == null ? request.getEntity().getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new)) : request.getColumns();
    }

    private Future<List<Entity>> getEntities(List<String> entityNames, String datamartMnemonic) {
        val futures = entityNames.stream()
                .map(name -> entityDao.getEntity(datamartMnemonic, name))
                .collect(Collectors.<Future>toList());
        return CompositeFuture.join(futures)
                .map(result -> {
                    List<Entity> entities = result.list();
                    return entities.stream()
                            .filter(e -> e.getEntityType() == EntityType.TABLE)
                            .sorted(Comparator.comparing(Entity::getName))
                            .collect(Collectors.toList());
                });
    }

    private Future<List<Long>> calcCheckSumForEntities(List<Entity> entities, CheckSumRequestContext request) {
        val checkFutures = entities.stream()
                .map(entity -> calcCheckSumTable(getNewRequestContext(entity, request)))
                .collect(Collectors.<Future>toList());
        return CompositeFuture.join(checkFutures)
                .map(CompositeFuture::list);
    }

    private CheckSumRequestContext getNewRequestContext(Entity entity, CheckSumRequestContext request) {
        val copyRequest = request.copy();
        copyRequest.setEntity(entity);
        return copyRequest;
    }
}
