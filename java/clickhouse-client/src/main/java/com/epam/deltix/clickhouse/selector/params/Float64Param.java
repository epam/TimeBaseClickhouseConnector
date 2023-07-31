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
package com.epam.deltix.clickhouse.selector.params;

import com.epam.deltix.clickhouse.schema.types.DataTypes;

public class Float64Param extends SelectParam {
    private final double float64Value;

    public Float64Param(double float64Value) {
        super(DataTypes.FLOAT64);
        this.float64Value = float64Value;
    }

    public double getFloat64Value() {
        return float64Value;
    }

    @Override
    public Object getValue() {
        return getFloat64Value();
    }
}