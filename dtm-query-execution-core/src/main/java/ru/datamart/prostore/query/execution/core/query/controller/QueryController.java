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
package ru.datamart.prostore.query.execution.core.query.controller;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.InputQueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.query.service.QueryAnalyzer;
import ru.datamart.prostore.query.execution.core.query.utils.LoggerContextUtils;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.UUID;

@Slf4j
@Component
public class QueryController {
    private final QueryAnalyzer queryAnalyzer;

    @Autowired
    public QueryController(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public void executeQuery(RoutingContext context) {
        InputQueryRequest inputQueryRequest = context.getBodyAsJson().mapTo(InputQueryRequest.class);
        prepareRequestId(inputQueryRequest);
        log.info("Execution request sent: [{}]", inputQueryRequest);
        execute(context, inputQueryRequest);
    }

    private void execute(RoutingContext context, InputQueryRequest inputQueryRequest) {
        AsyncUtils.measureMs(queryAnalyzer.analyzeAndExecute(inputQueryRequest),
                duration -> log.info("Request succeeded: [{}] in [{}]ms", inputQueryRequest.getSql(), duration))
                .onSuccess(queryResult -> {
                    if (queryResult.getRequestId() == null) {
                        queryResult.setRequestId(inputQueryRequest.getRequestId());
                    }
                    sendResponse(context, queryResult);
                })
                .onFailure(fail -> {
                    log.error("Error while executing request [{}]", inputQueryRequest, fail);
                    context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), fail);

                });
    }

    private void sendResponse(RoutingContext context, QueryResult queryResult) {
        try {
            val json = CoreSerialization.serializeAsString(queryResult);
            context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
        } catch (Exception e) {
            log.error("Error in serializing query result", e);
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), new DtmException(e));
        }
    }

    private void prepareRequestId(InputQueryRequest inputQueryRequest) {
        if(inputQueryRequest.getRequestId() == null) {
            inputQueryRequest.setRequestId(UUID.randomUUID());
        }
        LoggerContextUtils.setRequestId(inputQueryRequest.getRequestId());
    }
}
