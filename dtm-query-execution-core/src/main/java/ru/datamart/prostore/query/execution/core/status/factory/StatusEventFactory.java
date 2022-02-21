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
package ru.datamart.prostore.query.execution.core.status.factory;

import ru.datamart.prostore.common.status.PublishStatusEventRequest;
import ru.datamart.prostore.common.status.StatusEventCode;
import ru.datamart.prostore.query.execution.core.status.service.StatusEventFactoryRegistry;
import org.springframework.beans.factory.annotation.Autowired;

public interface StatusEventFactory<OUT> {
    PublishStatusEventRequest<OUT> create(String datamart, String eventData);

    StatusEventCode getEventCode();

    @Autowired
    default void registry(StatusEventFactoryRegistry registry) {
        registry.registryFactory(this);
    }

}
