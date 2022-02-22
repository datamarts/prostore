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
package ru.datamart.prostore.query.execution.plugin.api.request;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.api.dto.PluginRequest;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

import java.util.UUID;

@Getter
public class DdlRequest extends PluginRequest {

	private final SqlKind sqlKind;
	private final Entity entity;

	@Builder
	public DdlRequest(UUID requestId,
					  String envName,
					  String datamartMnemonic,
					  Entity entity,
					  SqlKind sqlKind) {
		super(requestId, envName, datamartMnemonic);
		this.entity = entity;
		this.sqlKind = sqlKind;
	}

	@Override
	public String toString() {
		return "DdlRequest{" +
				super.toString() +
				", classTable=" + entity +
				'}';
	}
}
