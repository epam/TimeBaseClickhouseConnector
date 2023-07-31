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
package com.epam.deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.qsrv.hf.pub.md.*;

public class AllNumericEncodingTypesTestMessage extends InstrumentMessage {

    protected int sequence;

    //
    //  FLOAT
    //
    protected float float_c_32;
    protected float float_n_32;
    protected double float_c_64;
    protected double float_n_64;
    protected double float_c_dec1;
    protected double float_n_dec1;
    protected double float_c_dec2;
    protected double float_n_dec2;

    //
    //  INTEGER
    //
    protected byte int_c_8;
    protected byte int_n_8;
    protected short int_c_16;
    protected short int_n_16;
    protected int int_c_32;
    protected int int_n_32;
    protected long int_c_64;
    protected long int_n_64;
    protected int puint_c_30;
    protected int puint_n_30;
    protected long puint_c_61;
    protected long puint_n_61;

    protected byte int_s_8;
    protected short int_s_16;
    protected int int_s_32;
    protected long int_s_64;

    /********************************************
     * GETTERS
     *********************************************/

    @SchemaElement(
            title = "Sequence"
    )
    public int getSequence() {
        return sequence;
    }

    //
    //  FLOAT
    //
    @SchemaElement(
            title = "Non-nullable FLOAT:IEEE32"
    )
    @SchemaType(
            encoding = "IEEE32",
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public float getFloat_c_32() {
        return float_c_32;
    }

    @SchemaElement(
            title = "Non-nullable FLOAT:IEEE64"
    )
    @SchemaType(
            encoding = "IEEE64",
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public double getFloat_c_64() {
        return float_c_64;
    }

    @SchemaElement(
            title = "Non-nullable FLOAT:DECIMAL(2)"
    )
    @SchemaType(
            encoding = "DECIMAL(2)",
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public double getFloat_c_dec2() {
        return float_c_dec2;
    }

    @SchemaElement(
            title = "Non-nullable FLOAT:DECIMAL"
    )
    @SchemaType(
            encoding = "DECIMAL",
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public double getFloat_c_dec1() {
        return float_c_dec1;
    }

    @SchemaElement(
            title = "Nullable FLOAT:IEEE32"
    )
    @SchemaType(
            encoding = "IEEE32",
            dataType = SchemaDataType.FLOAT
    )
    public float getFloat_n_32() {
        return float_n_32;
    }

    @SchemaElement(
            title = "Nullable FLOAT:IEEE64"
    )
    @SchemaType(
            encoding = "IEEE64",
            dataType = SchemaDataType.FLOAT
    )
    public double getFloat_n_64() {
        return float_n_64;
    }

    @SchemaElement(
            title = "Nullable FLOAT:DECIMAL(4)"
    )
    @SchemaType(
            encoding = "DECIMAL(4)",
            dataType = SchemaDataType.FLOAT
    )
    public double getFloat_n_dec2() {
        return float_n_dec2;
    }

    @SchemaElement(
            title = "Nullable FLOAT:DECIMAL"
    )
    @SchemaType(
            encoding = "DECIMAL",
            dataType = SchemaDataType.FLOAT
    )
    public double getFloat_n_dec1() {
        return float_n_dec1;
    }


    //
    //  INTEGER
    //
    @SchemaElement(
            title = "Non-nullable INTEGER:INT8"
    )
    @SchemaType(
            encoding = "INT8",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public byte getInt_c_8() {
        return int_c_8;
    }

    @SchemaElement()
    @SchemaType(
            encoding = "SIGNED(8)",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public byte getInt_s_8() {
        return int_s_8;
    }

    @SchemaElement()
    @SchemaType(
            encoding = "SIGNED(16)",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public short getInt_s_16() {
        return int_s_16;
    }

    @SchemaElement()
    @SchemaType(
            encoding = "SIGNED(32)",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public int getInt_s_32() {
        return int_s_32;
    }

    @SchemaElement()
    @SchemaType(
            encoding = "SIGNED(64)",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public long getInt_s_64() {
        return int_s_64;
    }

    @SchemaElement(
            title = "Nullable INTEGER:INT8"
    )
    @SchemaType(
            encoding = "INT8",
            dataType = SchemaDataType.INTEGER
    )
    public byte getInt_n_8() {
        return int_n_8;
    }

    @SchemaElement(
            title = "Non-nullable INTEGER:INT16"
    )
    @SchemaType(
            encoding = "INT16",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public short getInt_c_16() {
        return int_c_16;
    }

    @SchemaElement(
            title = "Nullable INTEGER:INT16"
    )
    @SchemaType(
            encoding = "INT16",
            dataType = SchemaDataType.INTEGER
    )
    public short getInt_n_16() {
        return int_n_16;
    }

    @SchemaElement(
            title = "Non-nullable INTEGER:INT32"
    )
    @SchemaType(
            encoding = "INT32",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public int getInt_c_32() {
        return int_c_32;
    }

    @SchemaElement(
            title = "Nullable INTEGER:INT32"
    )
    @SchemaType(
            encoding = "INT32",
            dataType = SchemaDataType.INTEGER
    )
    public int getInt_n_32() {
        return int_n_32;
    }

    @SchemaElement(
            title = "Non-nullable INTEGER:INT64"
    )
    @SchemaType(
            encoding = "INT64",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public long getInt_c_64() {
        return int_c_64;
    }

    @SchemaElement(
            title = "Nullable INTEGER:INT64"
    )
    @SchemaType(
            encoding = "INT64",
            dataType = SchemaDataType.INTEGER
    )
    public long getInt_n_64() {
        return int_n_64;
    }

    @SchemaElement(
            title = "Non-nullable INTEGER:PUINT30"
    )
    @SchemaType(
            encoding = "PUINT30",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public int getPuint_c_30() {
        return puint_c_30;
    }

    @SchemaElement(
            title = "Nullable INTEGER:PUINT30"
    )
    @SchemaType(
            encoding = "PUINT30",
            dataType = SchemaDataType.INTEGER
    )
    public int getPuint_n_30() {
        return puint_n_30;
    }

    @SchemaElement(
            title = "Non-nullable INTEGER:PUINT61"
    )
    @SchemaType(
            encoding = "PUINT61",
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public long getPuint_c_61() {
        return puint_c_61;
    }

    @SchemaElement(
            title = "Nullable INTEGER:PUINT61"
    )
    @SchemaType(
            encoding = "PUINT61",
            dataType = SchemaDataType.INTEGER
    )
    public long getPuint_n_61() {
        return puint_n_61;
    }

    /*****************************************************
     * SETTERS
     ****************************************************/

    public void setInt_s_8(byte int_s_8) {
        this.int_s_8 = int_s_8;
    }

    public void setInt_s_16(short int_s_16) {
        this.int_s_16 = int_s_16;
    }

    public void setInt_s_32(int int_s_32) {
        this.int_s_32 = int_s_32;
    }

    public void setInt_s_64(long int_s_64) {
        this.int_s_64 = int_s_64;
    }

    public void setFloat_c_32(float float_c_32) {
        this.float_c_32 = float_c_32;
    }

    public void setFloat_c_64(double float_c_64) {
        this.float_c_64 = float_c_64;
    }

    public void setFloat_c_dec2(double float_c_dec2) {
        this.float_c_dec2 = float_c_dec2;
    }

    public void setFloat_c_dec1(double float_c_dec1) {
        this.float_c_dec1 = float_c_dec1;
    }

    public void setFloat_n_32(float float_n_32) {
        this.float_n_32 = float_n_32;
    }

    public void setFloat_n_64(double float_n_64) {
        this.float_n_64 = float_n_64;
    }

    public void setFloat_n_dec2(double float_n_dec2) {
        this.float_n_dec2 = float_n_dec2;
    }

    public void setFloat_n_dec1(double float_n_dec1) {
        this.float_n_dec1 = float_n_dec1;
    }

    public void setInt_c_16(short int_c_16) {
        this.int_c_16 = int_c_16;
    }

    public void setInt_c_32(int int_c_32) {
        this.int_c_32 = int_c_32;
    }

    public void setInt_c_64(long int_c_64) {
        this.int_c_64 = int_c_64;
    }

    public void setInt_c_8(byte int_c_8) {
        this.int_c_8 = int_c_8;
    }

    public void setInt_n_16(short int_n_16) {
        this.int_n_16 = int_n_16;
    }

    public void setInt_n_32(int int_n_32) {
        this.int_n_32 = int_n_32;
    }

    public void setInt_n_64(long int_n_64) {
        this.int_n_64 = int_n_64;
    }

    public void setInt_n_8(byte int_n_8) {
        this.int_n_8 = int_n_8;
    }

    public void setPuint_c_30(int puint_c_30) {
        this.puint_c_30 = puint_c_30;
    }

    public void setPuint_c_61(long puint_c_61) {
        this.puint_c_61 = puint_c_61;
    }

    public void setPuint_n_30(int puint_n_30) {
        this.puint_n_30 = puint_n_30;
    }

    public void setPuint_n_61(long puint_n_61) {
        this.puint_n_61 = puint_n_61;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    public void fillMessage(int seqNo, boolean skipNullable) {

        sequence = seqNo;
        float_c_32 = seqNo + 0.25F;
        float_n_32 = skipNullable ? FloatDataType.IEEE32_NULL : float_c_32;
        float_c_64 = seqNo + 0.25;
        float_n_64 = skipNullable ? FloatDataType.IEEE64_NULL : float_c_64;
        float_c_dec1 = seqNo * 0.00001;
        float_n_dec1 = skipNullable ? FloatDataType.IEEE64_NULL : float_c_dec1;
        float_c_dec2 = seqNo * 0.005;
        float_n_dec2 = skipNullable ? FloatDataType.IEEE64_NULL : float_c_dec2;
        int_s_8 = (byte) seqNo;
        int_s_16 = (short) seqNo;
        int_s_32 = seqNo;
        int_s_64 = seqNo;
        int_c_8 = (byte) seqNo;
        int_c_16 = (short) seqNo;
        int_c_32 = seqNo;
        int_c_64 = seqNo;
        puint_c_30 = seqNo;
        puint_c_61 = seqNo;
        int_n_8 = skipNullable ? IntegerDataType.INT8_NULL : int_c_8;
        int_n_16 = skipNullable ? IntegerDataType.INT16_NULL : int_c_16;
        int_n_32 = skipNullable ? IntegerDataType.INT32_NULL : int_c_32;
        int_n_64 = skipNullable ? IntegerDataType.INT64_NULL : int_c_64;
        puint_n_30 = skipNullable ? IntegerDataType.PUINT30_NULL : puint_c_30;
        puint_n_61 = skipNullable ? IntegerDataType.PUINT61_NULL : puint_c_61;

    }


}