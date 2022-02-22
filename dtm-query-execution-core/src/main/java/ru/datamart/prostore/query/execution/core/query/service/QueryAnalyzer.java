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
package ru.datamart.prostore.query.execution.core.query.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.InputQueryRequest;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckCall;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigCall;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlChanges;
import ru.datamart.prostore.query.calcite.core.extension.delta.SqlDeltaCall;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlUseSchema;
import ru.datamart.prostore.query.calcite.core.extension.eddl.DropDatabase;
import ru.datamart.prostore.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.datamart.prostore.query.calcite.core.extension.edml.SqlRollbackCrashedWriteOps;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.core.query.factory.QueryRequestFactory;
import ru.datamart.prostore.query.execution.core.query.factory.RequestContextFactory;
import ru.datamart.prostore.query.execution.core.query.utils.DatamartMnemonicExtractor;
import ru.datamart.prostore.query.execution.core.query.utils.DefaultDatamartSetter;

@Slf4j
@Component
public class QueryAnalyzer {

    private final QueryDispatcher queryDispatcher;
    private final DefinitionService<SqlNode> definitionService;
    private final Vertx vertx;
    private final RequestContextFactory requestContextFactory;
    private final DatamartMnemonicExtractor datamartMnemonicExtractor;
    private final DefaultDatamartSetter defaultDatamartSetter;
    private final QuerySemicolonRemover querySemicolonRemover;
    private final QueryRequestFactory queryRequestFactory;

    @Autowired
    public QueryAnalyzer(QueryDispatcher queryDispatcher,
                         @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                         RequestContextFactory requestContextFactory,
                         @Qualifier("coreVertx") Vertx vertx,
                         DatamartMnemonicExtractor datamartMnemonicExtractor,
                         DefaultDatamartSetter defaultDatamartSetter,
                         QuerySemicolonRemover querySemicolonRemover,
                         QueryRequestFactory queryRequestFactory) {
        this.queryDispatcher = queryDispatcher;
        this.definitionService = definitionService;
        this.requestContextFactory = requestContextFactory;
        this.vertx = vertx;
        this.datamartMnemonicExtractor = datamartMnemonicExtractor;
        this.defaultDatamartSetter = defaultDatamartSetter;
        this.queryRequestFactory = queryRequestFactory;
        this.querySemicolonRemover = querySemicolonRemover;
    }

    public Future<QueryResult> analyzeAndExecute(InputQueryRequest execQueryRequest) {
        return AsyncUtils.measureMs(getParsedQuery(execQueryRequest),
                        duration -> log.debug("Request parsed [{}] in [{}]ms", execQueryRequest.getSql(), duration))
                .compose(parsedQuery -> AsyncUtils.measureMs(createRequestContext(parsedQuery),
                        duration -> log.debug("Created request context [{}] in [{}]ms", execQueryRequest.getSql(), duration)))
                .compose(queryDispatcher::dispatch);
    }

    private Future<ParsedQueryResponse> getParsedQuery(InputQueryRequest inputQueryRequest) {
        return Future.future(promise -> vertx.executeBlocking(it -> {
            try {
                val request = querySemicolonRemover.remove(queryRequestFactory.create(inputQueryRequest));
                SqlNode node = definitionService.processingQuery(request.getSql());
                it.complete(new ParsedQueryResponse(request, node));
            } catch (Exception e) {
                it.fail(new DtmException("Error parsing query", e));
            }
        }, false, promise));
    }

    private Future<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>> createRequestContext(ParsedQueryResponse parsedQueryResponse) {
        return Future.future(promise -> {
            SqlNode sqlNode = parsedQueryResponse.getSqlNode();
            QueryRequest queryRequest = parsedQueryResponse.getQueryRequest();
            if (hasSchema(sqlNode)) {
                if (!Strings.isEmpty(queryRequest.getDatamartMnemonic())) {
                    sqlNode = defaultDatamartSetter.set(sqlNode, queryRequest.getDatamartMnemonic());
                }
                val datamartMnemonic = datamartMnemonicExtractor.extract(sqlNode);
                queryRequest.setDatamartMnemonic(datamartMnemonic);
            }

            requestContextFactory.create(queryRequest, sqlNode)
                    .onComplete(promise);
        });
    }

    private boolean hasSchema(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlDropSchema)
                && !(sqlNode instanceof SqlCreateSchema)
                && !(sqlNode instanceof SqlCreateDatabase)
                && !(sqlNode instanceof DropDatabase)
                && !(sqlNode instanceof SqlDeltaCall)
                && !(sqlNode instanceof SqlUseSchema)
                && !(sqlNode instanceof SqlCheckCall)
                && !(sqlNode instanceof SqlRollbackCrashedWriteOps)
                && !(sqlNode instanceof SqlConfigCall)
                && !(sqlNode instanceof SqlChanges);
    }

    @Data
    private static final class ParsedQueryResponse {
        private final QueryRequest queryRequest;
        private final SqlNode sqlNode;
    }

}
