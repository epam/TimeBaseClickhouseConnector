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

public class DateTime64DataType extends BaseDataType {

    private final int precision;

    public DateTime64DataType() {
        this(3);
    }

    public DateTime64DataType(int precision) {
        super(DataTypes.DATE_TIME64);

        if (precision < 0 || precision > 18)
            throw new IllegalArgumentException(String.format("Precision %d is out of bounds.", precision));

        this.precision = precision;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public String getSqlDefinition() {
        return String.format("%s(%d)", type.getSqlDefinition(), precision);
    }

}