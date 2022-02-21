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
package ru.datamart.prostore.query.execution.plugin.api.service.shared.adg;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service
public class AdgSharedServiceStub implements AdgSharedService {

    public static final String STUBBED_EXCEPTION = "Disabled plugin [ADG]";

    @Override
    public Future<Void> prepareStaging(AdgSharedPrepareStagingRequest request) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public Future<Void> transferData(AdgSharedTransferDataRequest request) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public AdgSharedProperties getSharedProperties() {
        throw new DtmException(STUBBED_EXCEPTION);
    }
}
