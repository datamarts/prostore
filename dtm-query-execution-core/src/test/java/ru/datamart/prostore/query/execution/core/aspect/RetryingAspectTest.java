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
package ru.datamart.prostore.query.execution.core.aspect;

import io.vertx.core.Future;
import lombok.val;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RetryingAspectTest {
    private final RetryingAspect retryingAspect = new RetryingAspect();

    @Mock
    private ProceedingJoinPoint joinPoint;

    @Mock
    private RetryableFuture retryableFuture;

    @Mock
    private Signature signature;

    @BeforeEach
    void setUp() {
        lenient().when(retryableFuture.retries()).thenReturn(2);
        lenient().when(retryableFuture.cause()).thenReturn(new Class[]{BadVersionException.class});
        lenient().when(joinPoint.getTarget()).thenReturn(this);
        lenient().when(signature.getName()).thenReturn("mock");
        lenient().when(joinPoint.getSignature()).thenReturn(signature);
    }

    @Test
    void shouldSucceedWithoutRetries() throws Throwable {
        // arrange
        when(joinPoint.proceed()).thenReturn(Future.succeededFuture("1"));

        // act
        Future<?> result = (Future<?>) retryingAspect.around(joinPoint, retryableFuture);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertEquals("1", result.result());

        verify(joinPoint, times(1)).proceed();
        verifyNoMoreInteractions(joinPoint);
    }

    @Test
    void shouldFailWithoutRetriesWhenNotCause() throws Throwable {
        // arrange
        when(joinPoint.proceed()).thenReturn(Future.failedFuture("Failure"));

        // act
        Future<?> result = (Future<?>) retryingAspect.around(joinPoint, retryableFuture);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success, should fail when all retries failed");
        }
        assertEquals("Failure", result.cause().getMessage());

        verify(joinPoint, times(1)).proceed();
        verifyNoMoreInteractions(joinPoint);
    }

    @Test
    void shouldSucceedWhenOneRetry() throws Throwable {
        // arrange
        when(joinPoint.proceed())
                .thenReturn(Future.failedFuture(new BadVersionException()))
                .thenReturn(Future.succeededFuture("1"));

        // act
        Future<?> result = (Future<?>) retryingAspect.around(joinPoint, retryableFuture);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertEquals("1", result.result());

        verify(joinPoint, times(2)).proceed();
        verify(joinPoint).getTarget();
        verify(joinPoint).getSignature();
        verifyNoMoreInteractions(joinPoint);
    }

    @Test
    void shouldSucceedWhenLastRetry() throws Throwable {
        // arrange
        when(joinPoint.proceed())
                .thenReturn(Future.failedFuture(new BadVersionException()))
                .thenReturn(Future.failedFuture(new BadVersionException()))
                .thenReturn(Future.succeededFuture("1"));

        // act
        Future<?> result = (Future<?>) retryingAspect.around(joinPoint, retryableFuture);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertEquals("1", result.result());

        verify(joinPoint, times(3)).proceed();
        verify(joinPoint, times(2)).getTarget();
        verify(joinPoint, times(2)).getSignature();
        verifyNoMoreInteractions(joinPoint);
    }

    @Test
    void shouldSucceedWhenLastRetryFailed() throws Throwable {
        // arrange
        when(joinPoint.proceed())
                .thenReturn(Future.failedFuture(new BadVersionException("1")))
                .thenReturn(Future.failedFuture(new BadVersionException("2")))
                .thenReturn(Future.failedFuture(new BadVersionException("3")));

        // act
        val result = (Future<?>) retryingAspect.around(joinPoint, retryableFuture);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success, should fail when all retries failed");
        }
        assertEquals("KeeperErrorCode = BadVersion for 3", result.cause().getMessage());

        verify(joinPoint, times(3)).proceed();
        verify(joinPoint, times(3)).getTarget();
        verify(joinPoint, times(3)).getSignature();
        verifyNoMoreInteractions(joinPoint);
    }

    @Test
    void shouldFailWhenJointpointThrows() throws Throwable {
        // arrange
        when(joinPoint.proceed()).thenThrow(new RuntimeException("Failure"));

        // act
        Future<?> result = (Future<?>) retryingAspect.around(joinPoint, retryableFuture);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success, should fail when all retries failed");
        }
        assertEquals("Failure", result.cause().getMessage());

        verify(joinPoint, times(1)).proceed();
        verifyNoMoreInteractions(joinPoint);
    }
}