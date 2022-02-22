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
package ru.datamart.prostore.query.execution.plugin.adqm.mppw.configuration;

import ru.datamart.prostore.query.execution.plugin.adqm.mppw.AdqmMppwExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MppwConfiguration {

    @Bean("adqmMppwService")
    public MppwService mpprService(List<AdqmMppwExecutor> executors) {
        return new MppwServiceImpl<>(executors);
    }
}
