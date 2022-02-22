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
package ru.datamart.prostore.query.execution.core.aspect;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;


@Component
@Aspect
@Slf4j
public class RetryingAspect {

    @Pointcut("within(ru.datamart.prostore.query.execution.core.delta.repository.executor.*) && @annotation(retryable)")
    private void inDeltaExecutors(RetryableFuture retryable) {
    }

    @Around(value = "inDeltaExecutors(retryable)", argNames = "joinPoint,retryable")
    public Object around(ProceedingJoinPoint joinPoint, RetryableFuture retryable) {
        val promise = Promise.promise();
        val retriesLeftAtomic = new AtomicInteger(retryable.retries());
        executeOrRetry(joinPoint, promise, retriesLeftAtomic, retryable);
        return promise.future();
    }

    private void executeOrRetry(ProceedingJoinPoint joinPoint, Promise<Object> promise,
                                AtomicInteger retriesLeftAtomic, RetryableFuture retryable) {
        try {
            val future = (Future<?>) joinPoint.proceed();
            future.onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    val error = ar.cause();
                    int retriesLeft = retriesLeftAtomic.getAndDecrement();
                    if (retriesLeft > 0 && isAffected(error, retryable)) {
                        log.warn("Execution of [{}] failed. Retries left [{}/{}]. Retrying", getInvocationString(joinPoint), retriesLeft, retryable.retries());
                        executeOrRetry(joinPoint, promise, retriesLeftAtomic, retryable);
                    } else {
                        if (retriesLeft == 0) {
                            log.error("All retries [{}] of [{}] failed", retryable.retries(), getInvocationString(joinPoint));
                        }
                        promise.fail(error);
                    }
                }
            });
        } catch (Throwable e) {
            promise.fail(e);
        }
    }

    @NotNull
    private String getInvocationString(ProceedingJoinPoint joinPoint) {
        return joinPoint.getTarget().getClass().getSimpleName() + "#" + joinPoint.getSignature().getName();
    }

    private boolean isAffected(Throwable error, RetryableFuture retryable) {
        val allErrors = ExceptionUtils.getThrowableList(error);
        for (val throwable : allErrors) {
            for (val cause : retryable.cause()) {
                if (cause.isAssignableFrom(throwable.getClass())) {
                    return true;
                }
            }
        }

        return false;
    }

}
