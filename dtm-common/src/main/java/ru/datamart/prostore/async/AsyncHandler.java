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
package ru.datamart.prostore.async;

import ru.datamart.prostore.common.exception.DtmException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public interface AsyncHandler<T> extends Handler<AsyncResult<T>> {

    default void handleSuccess(T result) {
        handle(Future.succeededFuture(result));
    }

    default void handleSuccess() {
        handle(Future.succeededFuture());
    }

    default void handleError(String errMsg, Throwable error) {
        handle(Future.failedFuture(new DtmException(errMsg, error)));
    }

    default void handleError(String errMsg) {
        handle(Future.failedFuture(new DtmException(errMsg)));
    }

    default void handleError(Throwable error) {
        handle(Future.failedFuture(error));
    }
}
