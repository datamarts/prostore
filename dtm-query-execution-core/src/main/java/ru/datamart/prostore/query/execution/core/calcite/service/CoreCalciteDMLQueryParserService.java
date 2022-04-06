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
package ru.datamart.prostore.query.execution.core.calcite.service;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.calcite.core.provider.CalciteContextProvider;
import ru.datamart.prostore.query.calcite.core.service.SchemaExtender;
import ru.datamart.prostore.query.calcite.core.service.impl.CalciteDMLQueryParserService;

@Service("coreCalciteDMLQueryParserService")
public class CoreCalciteDMLQueryParserService extends CalciteDMLQueryParserService {

    @Autowired
    public CoreCalciteDMLQueryParserService(
            @Qualifier("coreCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx) {
        super(contextProvider, vertx, SchemaExtender.EMPTY);
    }
}
