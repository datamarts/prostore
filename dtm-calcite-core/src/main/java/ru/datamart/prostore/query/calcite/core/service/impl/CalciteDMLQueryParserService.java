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
package ru.datamart.prostore.query.calcite.core.service.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.springframework.core.NestedExceptionUtils;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.dialect.LimitSqlDialect;
import ru.datamart.prostore.query.calcite.core.provider.CalciteContextProvider;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.SchemaExtender;

@Slf4j
public class CalciteDMLQueryParserService implements QueryParserService {
    private static final LimitSqlDialect SQL_DIALECT = new LimitSqlDialect(CalciteSqlDialect.DEFAULT_CONTEXT);
    private final CalciteContextProvider contextProvider;
    private final Vertx vertx;
    private final SchemaExtender schemaExtender;

    public CalciteDMLQueryParserService(CalciteContextProvider contextProvider,
                                        Vertx vertx,
                                        SchemaExtender schemaExtender) {
        this.contextProvider = contextProvider;
        this.vertx = vertx;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<QueryParserResponse> parse(QueryParserRequest request) {
        return Future.future(promise -> vertx.executeBlocking(it -> {
            try {
                val datamarts = schemaExtender.extendSchema(request.getSchema(), request.getEnv());
                val context = contextProvider.context(datamarts);
                val sql = request.getQuery().toSqlString(getSqlDialect()).getSql();
                val parse = context.getPlanner().parse(sql);
                val validatedQuery = context.getPlanner().validate(parse);
                val relQuery = context.getPlanner().rel(validatedQuery);
                it.complete(new QueryParserResponse(
                        context,
                        datamarts,
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
}
