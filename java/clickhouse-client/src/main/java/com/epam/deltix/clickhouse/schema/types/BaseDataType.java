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
package com.epam.deltix.clickhouse.schema.types;

import com.epam.deltix.clickhouse.schema.SqlElement;

import java.util.List;

public abstract class BaseDataType implements SqlDataType {

    protected final DataTypes type;

    protected BaseDataType(DataTypes type) {
        this.type = type;
    }

    public DataTypes getType() {
        return type;
    }

    @Override
    public String getSqlDefinition() {
        return type.getSqlDefinition();
    }

    @Override
    public String toString() {
        return getSqlDefinition();
    }


    protected String getSqlDefinitionForComplexType(List<? extends SqlElement> nestedElements) {
        StringBuilder sb = new StringBuilder(type.getSqlDefinition());
        sb.append('(');

        for (int i = 0; i < nestedElements.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append(nestedElements.get(i).getSqlDefinition());
        }

        sb.append(')');
        return sb.toString();
    }

}