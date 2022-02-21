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
package ru.datamart.prostore.query.execution.core.delta.exception;

public class DeltaIsNotCommittedException extends DeltaException {
    private static final String MESSAGE = "The delta %d is not committed.";
    private static final String MESSAGE_WITHOUT_DELTA_NUM = "Delta is forbidden to start";

    public DeltaIsNotCommittedException() {
        super(MESSAGE);
    }

    public DeltaIsNotCommittedException(long deltaHot) {
        super(String.format(MESSAGE, deltaHot));
    }

    public DeltaIsNotCommittedException(long deltaHot, Throwable error) {
        super(String.format(MESSAGE, deltaHot), error);
    }

    public DeltaIsNotCommittedException(Throwable error) {
        super(MESSAGE_WITHOUT_DELTA_NUM, error);
    }
}
