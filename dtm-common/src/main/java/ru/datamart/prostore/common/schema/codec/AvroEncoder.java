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
package ru.datamart.prostore.common.schema.codec;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.util.List;

@Slf4j
public class AvroEncoder<T> extends AvroSerdeHelper {

    @SneakyThrows
    public byte[] encode(List<T> values, Schema schema) {
        try (val writer = new DataFileWriter<T>(new SpecificDatumWriter<>(schema))) {
            val baos = new ByteArrayOutputStream();
            writer.create(schema, baos);
            for (T value : values) {
                writer.append(value);
            }
            writer.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("AVRO serialization error", e);
            throw e;
        }
    }

    @SneakyThrows
    public byte[] encode(List<T> values, Schema schema, CodecFactory codec) {
        try (val writer = new DataFileWriter<T>(new SpecificDatumWriter<>(schema))) {
            val baos = new ByteArrayOutputStream();
            writer.setCodec(codec);
            writer.create(schema, baos);
            for (T value : values) {
                writer.append(value);
            }
            writer.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("AVRO serialization error", e);
            throw e;
        }
    }
}
