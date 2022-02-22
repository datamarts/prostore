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
package ru.datamart.prostore.async;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class AsyncUtils {

    private AsyncUtils() {
    }

    public static <T, R> Handler<AsyncResult<T>> succeed(Handler<AsyncResult<R>> handler, Consumer<T> succeed) {
        return ar -> {
            if (ar.succeeded()) {
                try {
                    succeed.accept(ar.result());
                } catch (Exception ex) {
                    log.error("Error: ", ex);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.error("Error: ", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        };
    }

    public static <T> Future<T> measureMs(Future<T> future, DurationListener durationListener) {
        long startTime = System.currentTimeMillis();
        return future.onSuccess(v -> durationListener.onDuration(System.currentTimeMillis() - startTime));
    }
}
