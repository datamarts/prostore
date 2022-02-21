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
package ru.datamart.prostore.query.calcite.core.service.impl;

import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.dialect.LimitSqlDialect;
import ru.datamart.prostore.query.calcite.core.provider.CalciteContextProvider;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.springframework.core.NestedExceptionUtils;

import java.util.List;

@Slf4j
public abstract class CalciteDMLQueryParserService implements QueryParserService {
    protected static final LimitSqlDialect SQL_DIALECT = new LimitSqlDialect(CalciteSqlDialect.DEFAULT_CONTEXT);
    protected final CalciteContextProvider contextProvider;
    protected final Vertx vertx;

    public CalciteDMLQueryParserService(CalciteContextProvider contextProvider,
                                        Vertx vertx) {
        this.contextProvider = contextProvider;
        this.vertx = vertx;
    }

    @Override
    public Future<QueryParserResponse> parse(QueryParserRequest request) {
        return Future.future(promise -> vertx.executeBlocking(it -> {
            try {
                val context = contextProvider.context(extendSchemes(request.getSchema()));
                val sql = request.getQuery().toSqlString(getSqlDialect()).getSql();
                val parse = context.getPlanner().parse(sql);
                val validatedQuery = context.getPlanner().validate(parse);
                val relQuery = context.getPlanner().rel(validatedQuery);
                it.complete(new QueryParserResponse(
                        context,
                        request.getSchema(),
                        relQuery,
                        validatedQuery
                ));
            } catch (Exception e) {
                String causeMsg = NestedExceptionUtils.getMostSpecificCause(e).getMessage();
                it.fail(new DtmException("Request parsing error: " + causeMsg, e));
            }
        }, false, ar -> {
            if (ar.succeeded()) {
                promise.complete((QueryParserResponse) ar.result());
            } else {
                promise.fail(ar.cause());
            }
        }));
    }

    protected SqlDialect getSqlDialect() {
        return SQL_DIALECT;
    }

    protected List<Datamart> extendSchemes(List<Datamart> datamarts) {
        return datamarts;
    }
}
