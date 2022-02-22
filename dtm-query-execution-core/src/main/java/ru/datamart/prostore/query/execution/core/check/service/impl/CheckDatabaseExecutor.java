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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckQueryResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("checkDatabaseExecutor")
public class CheckDatabaseExecutor implements CheckExecutor {

    private final EntityDao entityDao;
    private final DatamartDao datamartDao;
    private final CheckQueryResultFactory queryResultFactory;
    private final CheckTableService checkTableService;

    @Autowired
    public CheckDatabaseExecutor(EntityDao entityDao,
                                 DatamartDao datamartDao,
                                 CheckQueryResultFactory queryResultFactory,
                                 @Qualifier("coreCheckTableService") CheckTableService checkTableService) {
        this.entityDao = entityDao;
        this.datamartDao = datamartDao;
        this.queryResultFactory = queryResultFactory;
        this.checkTableService = checkTableService;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return datamartExists(datamartMnemonic)
                .compose(exist -> exist ? entityDao.getEntityNamesByDatamart(datamartMnemonic)
                        : Future.failedFuture(new DatamartNotExistsException(datamartMnemonic)))
                .compose(names -> getEntities(names, datamartMnemonic))
                .compose(entities -> checkEntities(entities, context))
                .map(queryResultFactory::create);
    }

    private Future<Boolean> datamartExists(String datamart) {
        return Future.future(promise -> datamartDao.getDatamart(datamart)
                .onSuccess(success -> promise.complete(true))
                .onFailure(error -> promise.complete(false)));
    }

    private Future<List<Entity>> getEntities(List<String> entityNames, String datamartMnemonic) {
        return Future.future(promise -> CompositeFuture.join(
                entityNames.stream()
                        .map(name -> entityDao.getEntity(datamartMnemonic, name))
                        .collect(Collectors.toList())
        )
                .onSuccess(result -> promise.complete(result.list()))
                .onFailure(promise::fail));
    }

    private Future<String> checkEntities(List<Entity> entities, CheckContext context) {
        return Future.future(promise -> CompositeFuture.join(
                entities.stream()
                        .filter(entity -> entity.getDestination() != null)
                        .filter(entity -> EntityType.TABLE.equals(entity.getEntityType()))
                        .map(entity -> checkTableService.checkEntity(entity, context))
                        .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<String> list = result.list();
                    promise.complete(String.join("\n", list));
                })
                .onFailure(promise::fail));
    }

    @Override
    public CheckType getType() {
        return CheckType.DATABASE;
    }
}
