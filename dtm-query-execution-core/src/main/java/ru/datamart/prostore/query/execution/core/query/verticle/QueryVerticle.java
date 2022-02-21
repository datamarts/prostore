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
package ru.datamart.prostore.query.execution.core.query.verticle;

import com.google.common.net.HttpHeaders;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.CoreHttpProperties;
import ru.datamart.prostore.query.execution.core.base.dto.request.RequestParam;
import ru.datamart.prostore.query.execution.core.metrics.controller.MetricsController;
import ru.datamart.prostore.query.execution.core.query.controller.DatamartMetaController;
import ru.datamart.prostore.query.execution.core.query.controller.QueryController;
import ru.datamart.prostore.query.execution.core.query.utils.ExceptionUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.util.MimeTypeUtils;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Slf4j
public class QueryVerticle extends AbstractVerticle {

    private final CoreHttpProperties httpProperties;
    private final DatamartMetaController datamartMetaController;
    private final QueryController queryController;
    private final MetricsController metricsController;

    public QueryVerticle(CoreHttpProperties httpProperties,
                         DatamartMetaController datamartMetaController,
                         QueryController queryController,
                         MetricsController metricsController) {
        this.httpProperties = httpProperties;
        this.datamartMetaController = datamartMetaController;
        this.queryController = queryController;
        this.metricsController = metricsController;
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);
        router.mountSubRouter("/", apiRouter());
        vertx.createHttpServer(
                        new HttpServerOptions()
                                .setTcpNoDelay(httpProperties.isTcpNoDelay())
                                .setTcpFastOpen(httpProperties.isTcpFastOpen())
                                .setTcpQuickAck(httpProperties.isTcpQuickAck())
                ).requestHandler(router)
                .listen(httpProperties.getPort());
    }

    private Router apiRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().consumes(APPLICATION_JSON_VALUE);
        router.route().produces(APPLICATION_JSON_VALUE);
        router.route().failureHandler(this::failureHandler);
        router.get("/meta").handler(datamartMetaController::getDatamartMeta);
        router.get(String.format("/meta/:%s/entities", RequestParam.DATAMART_MNEMONIC))
                .handler(datamartMetaController::getDatamartEntityMeta);
        router.get(String.format("/meta/:%s/entity/:%s/attributes", RequestParam.DATAMART_MNEMONIC, RequestParam.ENTITY_MNEMONIC))
                .handler(datamartMetaController::getEntityAttributesMeta);
        router.post("/query/execute").handler(queryController::executeQuery);
        router.put("/metrics/turn/on").handler(metricsController::turnOn);
        router.put("/metrics/turn/off").handler(metricsController::turnOff);
        return router;
    }

    private void failureHandler(RoutingContext ctx) {
        val failure = ctx.failure();
        val failureMessage = ExceptionUtils.prepareMessage(failure);
        val error = new JsonObject().put("exceptionMessage", failureMessage);
        ctx.response().setStatusCode(ctx.statusCode());
        ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE);
        ctx.response().end(error.encode());
    }
}
