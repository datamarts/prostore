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
package ru.datamart.prostore.common.eventbus;

import ru.datamart.prostore.common.newdata.DataMessage;

/*
 * Дто для передачи по шине с доп параметром deltaHot, чтобы не вмешиваться в мета данные
 * */
public class DataTempMessage extends DataMessage {

    Integer deltaHot;

    public DataTempMessage() {
    }

    public DataTempMessage(DataMessage dataMessage, Integer deltaHot) {
        this.setKey(dataMessage.getKey());
        this.setValue(dataMessage.getValue());
        this.setDeltaHot(deltaHot);
    }

    public Integer getDeltaHot() {
        return deltaHot;
    }

    public void setDeltaHot(Integer deltaHot) {
        this.deltaHot = deltaHot;
    }
}
