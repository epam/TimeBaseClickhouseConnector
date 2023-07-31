/*
 * Copyright 2023 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.clickhouse.selector.definitions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base interface for filters. Could represent filter expressions as well as `AND` and `OR` operators.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "$type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = FilterDefinitionImpl.class,            name = "FilterDefinition"),
        @JsonSubTypes.Type(value = AndFilterDefinitionImpl.class,         name = "AndFilterDefinition"),
        @JsonSubTypes.Type(value = OrFilterDefinitionImpl.class,          name = "OrFilterDefinition"),
        @JsonSubTypes.Type(value = SubqueryFilterDefinitionImpl.class,    name = "SubqueryFilterDefinition")}
)
public interface FilterNode<T> {
}