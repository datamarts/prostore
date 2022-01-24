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
package io.arenadata.dtm.query.execution.core.ddl.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlChanges;
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;
import io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaUtils;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.plugin.api.service.PostExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service("coreDdlService")
public class DdlService implements DatamartExecutionService<DdlRequestContext> {

    private final Map<String, DdlExecutor> executorMap;
    private final Map<PostSqlActionType, PostExecutor<DdlRequestContext>> postExecutorMap;
    private final ParseQueryUtils parseQueryUtils;

    @Autowired
    public DdlService(ParseQueryUtils parseQueryUtils,
                      List<PostExecutor<DdlRequestContext>> postExecutors,
                      List<DdlExecutor> ddlExecutors) {
        this.parseQueryUtils = parseQueryUtils;
        this.executorMap = new HashMap<>();
        this.postExecutorMap = postExecutors.stream()
                .collect(Collectors.toMap(PostExecutor::getPostActionType, Function.identity()));
        ddlExecutors.forEach(this::addExecutor);
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context) {
        return getExecutor(context)
                .compose(executor -> {
                    context.getPostActions().addAll(executor.getPostActions());
                    return executor.execute(context, getDatamartName(context));
                })
                .map(queryResult -> {
                    executePostActions(context);
                    return queryResult;
                });
    }

    private String getDatamartName(DdlRequestContext context) {
        if (context.getSqlCall() instanceof SqlChanges) {
            return null;
        }

        String datamartName = parseQueryUtils.getDatamartName(context.getSqlCall().getOperandList());
        checkEntityName(context.getRequest().getQueryRequest().getDatamartMnemonic(), datamartName, context.getSqlNode());
        return datamartName;
    }

    private void checkEntityName(String requestDatamart, String sqlNodeName, SqlNode sqlNode) {
        if (containsOnlyDatamartName(sqlNode)) {
            if (sqlNodeName.equalsIgnoreCase(InformationSchemaUtils.INFORMATION_SCHEMA)) {
                throw informationSchemaValidationException();
            }
            if (!sqlNodeName.matches("^[a-zA-Z]\\w*$")) {
                throw new ValidationDtmException("Unsupported database name. \n" +
                        "A name must start with a letter; " +
                        "the rest of the string can contain letters, digits, and underscores.");
            }
        } else {
            int indexComma = sqlNodeName.indexOf(".");
            String datamartName = indexComma == -1 ? requestDatamart : sqlNodeName.substring(0, indexComma);
            if (datamartName.equalsIgnoreCase(InformationSchemaUtils.INFORMATION_SCHEMA)) {
                throw informationSchemaValidationException();
            }
        }
    }

    private ValidationDtmException informationSchemaValidationException() {
        return new ValidationDtmException(String.format("DDL operations in the schema [%s] are not supported",
                InformationSchemaUtils.INFORMATION_SCHEMA));
    }

    private boolean containsOnlyDatamartName(SqlNode sqlNode) {
        return sqlNode instanceof SqlDropSchema || sqlNode instanceof SqlCreateSchema
                || sqlNode instanceof SqlCreateDatabase || sqlNode instanceof DropDatabase;
    }

    private Future<DdlExecutor> getExecutor(DdlRequestContext context) {
        return Future.future(promise -> {
            SqlCall sqlCall = getSqlCall(context.getSqlNode());
            context.setSqlCall(sqlCall);
            DdlExecutor executor = executorMap.get(sqlCall.getOperator().getName());
            if (executor != null) {
                promise.complete(executor);
            } else {
                promise.fail(new DtmException(String.format("Not supported DDL query type [%s]",
                        context.getSqlNode())));
            }
        });
    }

    private void executePostActions(DdlRequestContext context) {
        CompositeFuture.join(context.getPostActions().stream()
                .distinct()
                .map(postType -> Optional.ofNullable(postExecutorMap.get(postType))
                        .map(postExecutor -> postExecutor.execute(context))
                        .orElse(Future.failedFuture(new DtmException(String.format("Not supported DDL post executor type [%s]",
                                postType)))))
                .collect(Collectors.toList()))
                .onFailure(error -> log.error(error.getMessage()));
    }

    private SqlCall getSqlCall(SqlNode sqlNode) {
        if (sqlNode instanceof SqlAlter ||
                sqlNode instanceof SqlDdl ||
                sqlNode instanceof SqlBaseTruncate ||
                sqlNode instanceof SqlChanges) {
            return (SqlCall) sqlNode;
        } else {
            throw new DtmException("Not supported request type");
        }
    }

    private void addExecutor(DdlExecutor executor) {
        DdlExecutor alreadyRegistered = executorMap.put(executor.getOperationKind(), executor);
        if (alreadyRegistered != null) {
            throw new IllegalArgumentException(String.format("Duplicate executor for %s, same mapping: %s ->%s",
                    executor.getOperationKind(), executor.getClass().getSimpleName(), alreadyRegistered.getClass().getSimpleName()));
        }
    }

    @Override
    public SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DDL;
    }
}
