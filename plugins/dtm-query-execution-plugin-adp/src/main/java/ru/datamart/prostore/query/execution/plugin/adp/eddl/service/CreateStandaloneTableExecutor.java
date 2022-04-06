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
package ru.datamart.prostore.query.execution.plugin.adp.eddl.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.eddl.factory.AdpStandaloneQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.EddlExecutor;

@Service
public class CreateStandaloneTableExecutor implements EddlExecutor {
    private final DatabaseExecutor adpQueryExecutor;

    public CreateStandaloneTableExecutor(@Qualifier("adpQueryExecutor") DatabaseExecutor adpQueryExecutor) {
        this.adpQueryExecutor = adpQueryExecutor;
    }

    @Override
    public Future<Void> execute(EddlRequest request) {
        return Future.future(promise -> {
            val sql = AdpStandaloneQueriesFactory.createQuery(request.getEntity());
            adpQueryExecutor.executeUpdate(sql)
                    .onComplete(promise);
        });
    }

}
