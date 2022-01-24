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
package io.arenadata.dtm.status.monitor.version;

import io.arenadata.dtm.common.version.VersionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;

@Component
public class VersionService {

    private static final String STATUS_MONITOR_COMPONENT_NAME = "status-monitor";
    private final BuildProperties buildProperties;

    @Autowired
    public VersionService(BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    public VersionInfo getVersionInfo() {
        return new VersionInfo(STATUS_MONITOR_COMPONENT_NAME, buildProperties.getVersion());
    }
}
