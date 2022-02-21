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

/*DTO цельного сообщения по шине*/
public class DataMessage {
    DataMessageRequestKey key;
    DataMessageRequestValue value;

    public DataMessage() {
    }

    public DataMessage(DataMessageRequestKey key, DataMessageRequestValue value) {
        this.key = key;
        this.value = value;
    }

    public DataMessageRequestKey getKey() {
        return key;
    }

    public void setKey(DataMessageRequestKey key) {
        this.key = key;
    }

    public DataMessageRequestValue getValue() {
        return value;
    }

    public void setValue(DataMessageRequestValue value) {
        this.value = value;
    }
}
