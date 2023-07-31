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
package com.epam.deltix.clickhouse.selector.filters;

import com.epam.deltix.clickhouse.schema.types.DataTypes;

/**
 * Type of the filter that defines how the filter is applied.
 */
public enum FilterType {
    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING,
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DECIMAL,
        DataTypes.DECIMAL32,
        DataTypes.DECIMAL64,
        DataTypes.DECIMAL128,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
        DataTypes.ENUM8,
        DataTypes.ENUM16 })
    EQUAL,

    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING,
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DECIMAL,
        DataTypes.DECIMAL32,
        DataTypes.DECIMAL64,
        DataTypes.DECIMAL128,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
        DataTypes.ENUM8,
        DataTypes.ENUM16 })
    NOT_EQUAL,

    @ApplicableTypes({
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DECIMAL,
        DataTypes.DECIMAL32,
        DataTypes.DECIMAL64,
        DataTypes.DECIMAL128,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
    })
    LESS,

    @ApplicableTypes({
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DECIMAL,
        DataTypes.DECIMAL32,
        DataTypes.DECIMAL64,
        DataTypes.DECIMAL128,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
    })
    LESS_OR_EQUAL,

    @ApplicableTypes({
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DECIMAL,
        DataTypes.DECIMAL32,
        DataTypes.DECIMAL64,
        DataTypes.DECIMAL128,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
    })
    GREATER,

    @ApplicableTypes({
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DECIMAL,
        DataTypes.DECIMAL32,
        DataTypes.DECIMAL64,
        DataTypes.DECIMAL128,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
    })
    GREATER_OR_EQUAL,

    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING })
    CONTAINS,

    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING })
    STARTS_WITH,

    @ApplicableTypes({
         DataTypes.STRING,
        DataTypes.FIXED_STRING })
    ENDS_WITH,

    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING })
    NOT_CONTAINS,

    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING,
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
        DataTypes.ENUM8,
        DataTypes.ENUM16 })
    IN,

    @ApplicableTypes({
        DataTypes.STRING,
        DataTypes.FIXED_STRING,
        DataTypes.UINT8,
        DataTypes.UINT16,
        DataTypes.UINT32,
        DataTypes.UINT64,
        DataTypes.INT8,
        DataTypes.INT16,
        DataTypes.INT32,
        DataTypes.INT64,
        DataTypes.FLOAT32,
        DataTypes.FLOAT64,
        DataTypes.DATE,
        DataTypes.DATE_TIME,
        DataTypes.DATE_TIME64,
        DataTypes.ENUM8,
        DataTypes.ENUM16 })
    NOT_IN
}