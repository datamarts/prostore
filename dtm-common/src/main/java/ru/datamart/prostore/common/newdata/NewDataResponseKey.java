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
package ru.datamart.prostore.common.newdata;

import java.util.UUID;

public class NewDataResponseKey {
    private UUID loadProcID;
    private int sinId;
    private String schemaName;

    public NewDataResponseKey(UUID loadProcID, int sinId, String schemaName) {
        this.loadProcID = loadProcID;
        this.sinId = sinId;
        this.schemaName = schemaName;
    }

    public NewDataResponseKey() {
    }

    public UUID getLoadProcID() {
        return loadProcID;
    }

    public void setLoadProcID(UUID loadProcID) {
        this.loadProcID = loadProcID;
    }

    public int getSinId() {
        return sinId;
    }

    public void setSinId(int sinId) {
        this.sinId = sinId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
