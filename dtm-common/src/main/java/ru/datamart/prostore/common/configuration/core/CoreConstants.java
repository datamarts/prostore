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
package ru.datamart.prostore.common.configuration.core;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;

public final class CoreConstants {
    private CoreConstants() {
    }

    public static final TimeZone CORE_TIME_ZONE = TimeZone.getTimeZone("UTC");
    public static final ZoneId CORE_ZONE_ID = CORE_TIME_ZONE.toZoneId();
    public static final ZoneOffset CORE_ZONE_OFFSET = ZoneOffset.UTC;
    public static final int UUID_SIZE = 36;
}
