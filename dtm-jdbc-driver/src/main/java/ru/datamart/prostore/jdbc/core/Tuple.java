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
package ru.datamart.prostore.jdbc.core;

public class Tuple {

    final Object[] data;

    public Tuple(int length) {
        this(new Object[length]);
    }

    public Tuple(Object[] data) {
        this.data = data;
    }

    public int fieldCount() {
        return this.data.length;
    }

    public Object get(int index) {
        return this.data[index];
    }

    public void set(int index, Object fieldData) {
        this.data[index] = fieldData;
    }
}
