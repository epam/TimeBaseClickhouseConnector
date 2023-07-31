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

import java.util.Collections;
import java.util.List;

public class Enum16DataType extends BaseDataType {

    public static class Enum16Value {

        private final String name;
        private final short value;

        public Enum16Value(String name, short value) {
            if (name == null)
                throw new IllegalArgumentException("name");

            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public short getValue() {
            return value;
        }
    }


    private final List<Enum16Value> values;

    public Enum16DataType(List<Enum16Value> values) {
        super(DataTypes.ENUM16);

        this.values = Collections.unmodifiableList(values);
    }

    public List<Enum16Value> getValues() {
        return values;
    }

    @Override
    public String getSqlDefinition() {
        StringBuilder sb = new StringBuilder(type.getSqlDefinition());
        sb.append('(');

        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(", ");

            Enum16Value value = values.get(i);
            sb.append('\'');
            sb.append(value.name);
            sb.append('\'');
            sb.append(" = ");
            sb.append(value.value);
        }

        sb.append(')');
        return sb.toString();
    }
}