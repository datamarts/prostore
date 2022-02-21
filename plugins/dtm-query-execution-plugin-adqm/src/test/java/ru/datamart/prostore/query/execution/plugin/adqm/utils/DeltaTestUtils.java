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
package ru.datamart.prostore.query.execution.plugin.adqm.utils;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.delta.SelectOnInterval;

public final class DeltaTestUtils {
    private DeltaTestUtils() {
    }

    public static DeltaInformation deltaNum(long deltaNum) {
        return DeltaInformation.builder()
                .type(DeltaType.NUM)
                .selectOnNum(deltaNum)
                .build();
    }

    public static DeltaInformation deltaWithout(long deltaNum) {
        return DeltaInformation.builder()
                .selectOnNum(deltaNum)
                .type(DeltaType.WITHOUT_SNAPSHOT)
                .isLatestUncommittedDelta(true)
                .build();
    }

    public static DeltaInformation deltaOnDate(long deltaNum) {
        return DeltaInformation.builder()
                .selectOnNum(deltaNum)
                .type(DeltaType.DATETIME)
                .build();
    }

    public static DeltaInformation deltaStartedIn(long fromNum, long toNum) {
        return DeltaInformation.builder()
                .type(DeltaType.STARTED_IN)
                .selectOnInterval(new SelectOnInterval(fromNum, toNum))
                .build();
    }

    public static DeltaInformation deltaFinishedIn(long fromNum, long toNum) {
        return DeltaInformation.builder()
                .type(DeltaType.FINISHED_IN)
                .selectOnInterval(new SelectOnInterval(fromNum, toNum))
                .build();
    }
}
