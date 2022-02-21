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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.dto.DatamartInfo;
import ru.datamart.prostore.common.dto.schema.DatamartSchemaKey;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class LogicalSchemaService {

    private final EntityDao entityDao;
    private final DeltaInformationExtractor deltaInformationExtractor;

    @Autowired
    public LogicalSchemaService(ServiceDbFacade serviceDbFacade,
                                DeltaInformationExtractor deltaInformationExtractor) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaInformationExtractor = deltaInformationExtractor;
    }

    public Future<Map<DatamartSchemaKey, Entity>> createSchemaFromQuery(SqlNode query, String defaultDatamart) {
        return Future.future(promise -> {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoListFromQuery(query, defaultDatamart);
            createSchema(promise, datamartInfoList);
        });
    }

    public Future<Map<DatamartSchemaKey, Entity>> createSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations) {
        return Future.future(promise -> {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoListFromDeltaInformations(deltaInformations);
            createSchema(promise, datamartInfoList);
        });
    }

    private List<DatamartInfo> getDatamartInfoListFromQuery(SqlNode query, String defaultDatamart) {
        val tree = new SqlSelectTree(query);
        val datamartMap = new HashMap<String, DatamartInfo>();
        tree.findAllTableAndSnapshots().stream()
                .map(node -> deltaInformationExtractor.getDeltaInformation(tree, node))
                .filter(Objects::nonNull)
                .forEach(node -> {
                    //it is assumed that at this stage, the request will already contain defaultDatamart where required
                    String schemaName = getSchemaName(node, defaultDatamart);
                    String tableName = node.getTableName();
                    DatamartInfo datamartInfo = datamartMap.getOrDefault(schemaName, new DatamartInfo(schemaName, new HashSet<>()));
                    datamartInfo.getTables().add(tableName);
                    datamartMap.putIfAbsent(datamartInfo.getSchemaName(), datamartInfo);
                });
        return new ArrayList<>(datamartMap.values());
    }

    private String getSchemaName(DeltaInformation node, String defaultDatamart) {
        return (node.getSchemaName() == null || node.getSchemaName().isEmpty()) ? defaultDatamart : node.getSchemaName();
    }

    private List<DatamartInfo> getDatamartInfoListFromDeltaInformations(List<DeltaInformation> deltaInformations) {
        val datamartMap = new HashMap<String, DatamartInfo>();
        deltaInformations
                .forEach(d -> {
                    DatamartInfo datamartInfo = datamartMap.getOrDefault(d.getSchemaName(),
                            new DatamartInfo(d.getSchemaName(), new HashSet<>()));
                    datamartInfo.getTables().add(d.getTableName());
                    datamartMap.putIfAbsent(datamartInfo.getSchemaName(), datamartInfo);
                });
        return new ArrayList<>(datamartMap.values());
    }

    private void createSchema(Promise<Map<DatamartSchemaKey, Entity>> promise,
                              List<DatamartInfo> datamartInfoList) {
        CompositeFuture.join(
                datamartInfoList.stream()
                        .flatMap(di -> di.getTables().stream()
                                .map(tableName -> new DatamartSchemaKey(di.getSchemaName(), tableName)))
                        .map(dsKey -> entityDao.getEntity(dsKey.getSchema(), dsKey.getTable()))
                        .collect(Collectors.toList()))
                .onSuccess(success -> {
                    List<Entity> entities = success.list();
                    val schemaKeyDatamartTableMap = entities.stream()
                            .map(Entity::copy)
                            .collect(Collectors.toMap(this::createDatamartSchemaKey, Function.identity()));
                    promise.complete(schemaKeyDatamartTableMap);
                })
                .onFailure(error -> promise.fail(new DtmException("Error initializing table attributes", error)));
    }

    private DatamartSchemaKey createDatamartSchemaKey(Entity table) {
        return new DatamartSchemaKey(table.getSchema(), table.getName());
    }

}
