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
package ru.datamart.prostore.query.execution.core.ddl.dto;

import lombok.ToString;

@ToString
public enum DdlType {
	UNKNOWN(false),
	CREATE_SCHEMA(false),
	DROP_SCHEMA(false),
	CREATE_TABLE(true),
	DROP_TABLE(false),
	CREATE_VIEW(false),
	DROP_VIEW(false),
	CREATE_MATERIALIZED_VIEW(true),
	DROP_MATERIALIZED_VIEW(false),
	;

	boolean createTopic;

	DdlType(boolean createTopic) {
		this.createTopic = createTopic;
	}

	public boolean isCreateTopic() {
		return createTopic;
	}

}
