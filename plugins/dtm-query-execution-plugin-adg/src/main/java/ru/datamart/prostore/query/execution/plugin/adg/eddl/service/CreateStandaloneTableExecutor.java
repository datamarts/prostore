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
package ru.datamart.prostore.query.execution.plugin.adg.eddl.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.eddl.factory.AdgStandaloneQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.EddlExecutor;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.BUCKET_ID;

@Service
public class CreateStandaloneTableExecutor implements EddlExecutor {
    private final AdgCartridgeClient client;
    private final AdgStandaloneQueriesFactory createStandaloneFactory;

    public CreateStandaloneTableExecutor(AdgCartridgeClient client,
                                         AdgStandaloneQueriesFactory createStandaloneFactory) {
        this.client = client;
        this.createStandaloneFactory = createStandaloneFactory;
    }

    @Override
    public Future<Void> execute(EddlRequest request) {
        return Future.future(promise -> {
            checkBucketField(request.getEntity());
            val yaml = createStandaloneFactory.create(request.getEntity());
            client.executeCreateSpacesQueued(yaml)
                    .onComplete(promise);
        });
    }

    private void checkBucketField(Entity entity) {
        if (entity.getFields().stream()
                .noneMatch(field -> field.getName().equalsIgnoreCase(BUCKET_ID))) {
            throw new DtmException("bucket_id not found");
        }
    }

}
