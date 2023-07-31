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
package com.epam.deltix.clickhouse.writer;

import com.clickhouse.data.ClickHouseDataType;
import com.epam.deltix.clickhouse.writer.codec.CodecUtil;
import com.epam.deltix.clickhouse.models.BindDeclaration;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.SchemaArrayElement;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.schema.types.DataTypes;
import com.epam.deltix.clickhouse.schema.types.NestedDataType;
import com.epam.deltix.clickhouse.schema.types.NullableDataType;
import com.epam.deltix.clickhouse.util.BindHelper;
import com.epam.deltix.dfp.Decimal64;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static net.bytebuddy.jar.asm.Opcodes.*;

public class WriterCodecGenerator implements ByteCodeAppender {
    private final static String INTERNAL_PS_NAME = "java/sql/PreparedStatement";
    private final static String INTERNAL_LIST_NAME = "java/util/List";

    private final List<BindDeclaration> binds;
    private final IntrospectionType introspectionType;

    private final String internalClassName;

    private String generatedClassName;
    private int localVarSize;

    public WriterCodecGenerator(Class clazz,
                                List<BindDeclaration> binds,
                                IntrospectionType introspectionType) {
        this.binds = binds;
        this.introspectionType = introspectionType;

        internalClassName = Type.getInternalName(clazz);
    }

    @Override
    public Size apply(MethodVisitor methodVisitor,
                      Implementation.Context implementationContext,
                      MethodDescription instrumentedMethod) {
        generatedClassName = implementationContext.getInstrumentedType().getCanonicalName();
        localVarSize = instrumentedMethod.getStackSize();
        // load a reference (`clazz`) onto the stack from a local variable #1
        methodVisitor.visitVarInsn(ALOAD, 1);
        // checks whether an object reference is of a certain type,
        // the class reference of which is in the constant pool at index #1
        methodVisitor.visitTypeInsn(CHECKCAST, internalClassName);
        // store a reference(`clazz`) into a local variable #3
        methodVisitor.visitVarInsn(ASTORE, 3);
        localVarSize++;

        // initialize 'parameterIndex' variable with value - 1;
        methodVisitor.visitInsn(ICONST_1);
        storeParameterIndexValue(methodVisitor);

        Label l1 = new Label();
        methodVisitor.visitLabel(l1);

        for (BindDeclaration bind : binds) {
            String memberName = BindHelper.memberName.apply(bind);
            Class<?> memberDataType = BindHelper.memberType.apply(bind);

            String memberTypeDescriptor = BindHelper.memberDescriptor.apply(bind);

            SqlDataType dbDataType = bind.getDbDataType();
            DataTypes dbDataTypeValue = dbDataType.getType();

            SchemaArrayElement elementAnnotation = BindHelper.memberArrayElement.apply(bind);

            processAnyType(methodVisitor, memberDataType, memberName, memberTypeDescriptor,
                    elementAnnotation, dbDataType, dbDataTypeValue);
        }

        loadPsOnStack(methodVisitor);
        methodVisitor.visitInsn(ARETURN);

        Label l2 = new Label();
        methodVisitor.visitLabel(l2);

        methodVisitor.visitLocalVariable("parameterIndex", "I", null, l1, l2, 4);

        return new Size(4, localVarSize);
    }

    private void processAnyType(MethodVisitor methodVisitor, Class<?> memberDataType, String memberName,
                                String memberTypeDescriptor, SchemaArrayElement elementAnnotation,
                                SqlDataType dbDataType, DataTypes dbDataTypeValue) {
        switch (dbDataTypeValue) {
            case ARRAY:
                invokeMemberValueValidation(methodVisitor, memberName, memberTypeDescriptor);
                processArray(methodVisitor, elementAnnotation, memberName, memberDataType, memberTypeDescriptor);
                break;
            case NESTED:
                processNested(methodVisitor, elementAnnotation.nestedType(), memberName, memberTypeDescriptor, dbDataType);
                break;
            case TUPLE:
                throw unsupportedDataTypeException(dbDataTypeValue, memberDataType);

            case NULLABLE:
                dbDataTypeValue = ((NullableDataType) dbDataType).getNestedType().getType();

                processSimpleType(methodVisitor, memberName, memberDataType, memberTypeDescriptor, dbDataTypeValue);
                break;

            default:
                processSimpleType(methodVisitor, memberName, memberDataType, memberTypeDescriptor, dbDataTypeValue);
        }
    }

    private void processNested(MethodVisitor methodVisitor,
                               Class<?> nestedClass, String memberName,
                               String memberTypeDescriptor, SqlDataType dbDataType) {
        String internalNestedName = Type.getInternalName(nestedClass);

        List<ColumnDeclaration> columns = ((NestedDataType) dbDataType).getColumns();

        loadMemberValueOnStack(methodVisitor, memberName, memberTypeDescriptor);
        storeNestedMemberValue(methodVisitor);

        loadNestedMemberOnStack(methodVisitor);
        methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.MAKE_LIST_NOT_NULL_NAME, CodecUtil.MAKE_LIST_NOT_NULL_DESC, false);
        storeNestedMemberValue(methodVisitor);

        loadNestedMemberOnStack(methodVisitor);
        // invoke size() method on a `memberName`
        methodVisitor.visitMethodInsn(INVOKEINTERFACE, INTERNAL_LIST_NAME, "size", "()I", true);
        storeSizeOfNestedMemberValue(methodVisitor);

        List<String> arrayDescriptors = new ArrayList<>();
        List<BindDeclaration> nestedBinds;
        if (IntrospectionType.BY_GETTERS == introspectionType)
            nestedBinds = BindHelper.getBindDeclarationsByGetMethods(nestedClass, columns);
        else
            nestedBinds = BindHelper.getBindDeclarationsByFields(nestedClass, columns);

        int currentLocalVarIndex = 7;
        for (BindDeclaration bind : nestedBinds) {
            Class<?> nestedType = BindHelper.memberType.apply(bind);
            String nestedInternalName = Type.getInternalName(nestedType);

            arrayDescriptors.add(String.format("[%s", Type.getDescriptor(nestedType)));

            currentLocalVarIndex = initArray(methodVisitor, nestedType, nestedInternalName, currentLocalVarIndex);
        }

        Label start = new Label();
        Label loop = new Label();

        methodVisitor.visitLabel(start);
        methodVisitor.visitInsn(ICONST_0);
        int tempVarIndex = currentLocalVarIndex;
        // it is 'index' in 'for' loop.
        methodVisitor.visitVarInsn(ISTORE, tempVarIndex);
        localVarSize++;

        methodVisitor.visitLabel(loop);

        // '+ 1' need, because 'this' reference is also local variable.
        int numLocal = tempVarIndex + 1;

        Object[] localVariableTypes = getLocalVarTypes(arrayDescriptors, numLocal);

        methodVisitor.visitFrame(F_FULL, numLocal, localVariableTypes, 0, new Object[]{});

        methodVisitor.visitVarInsn(ILOAD, tempVarIndex);
        loadSizeOfNestedMemberValueOnStack(methodVisitor);
        Label l6 = new Label();
        methodVisitor.visitJumpInsn(IF_ICMPGE, l6);
        Label l7 = new Label();
        methodVisitor.visitLabel(l7);

        currentLocalVarIndex = 7;
        for (BindDeclaration bind : nestedBinds) {
            String nestedName = BindHelper.memberName.apply(bind);
            Class<?> nestedType = BindHelper.memberType.apply(bind);

            String nestedTypeDescriptor = BindHelper.memberDescriptor.apply(bind);

            methodVisitor.visitVarInsn(ALOAD, currentLocalVarIndex++);
            methodVisitor.visitVarInsn(ILOAD, tempVarIndex);

            loadNestedMemberOnStack(methodVisitor);

            methodVisitor.visitVarInsn(ILOAD, tempVarIndex);
            methodVisitor.visitMethodInsn(INVOKEINTERFACE, INTERNAL_LIST_NAME, "get", "(I)Ljava/lang/Object;", true);
            methodVisitor.visitTypeInsn(CHECKCAST, internalNestedName);
            loadNestedMemberValueOnStack(methodVisitor, internalNestedName, nestedName, nestedTypeDescriptor);

            if (!nestedType.isPrimitive())
                methodVisitor.visitInsn(AASTORE);
            else if (nestedType.equals(long.class))
                methodVisitor.visitInsn(LASTORE);
            else if (nestedType.equals(int.class))
                methodVisitor.visitInsn(IASTORE);
            else if (nestedType.equals(short.class))
                methodVisitor.visitInsn(SASTORE);
            else if (nestedType.equals(float.class))
                methodVisitor.visitInsn(FASTORE);
            else if (nestedType.equals(double.class))
                methodVisitor.visitInsn(DASTORE);
            else if (nestedType.equals(byte.class) ||
                    nestedType.equals(boolean.class))
                methodVisitor.visitInsn(BASTORE);

        }

        Label l8 = new Label();
        methodVisitor.visitLabel(l8);
        methodVisitor.visitIincInsn(tempVarIndex, 1);
        methodVisitor.visitJumpInsn(GOTO, loop);
        methodVisitor.visitLabel(l6);
        methodVisitor.visitFrame(F_CHOP, 1, null, 0, null);

        currentLocalVarIndex = 7;
        for (BindDeclaration bind : nestedBinds) {
            Class<?> nestedType = BindHelper.memberType.apply(bind);

            ClickHouseDataType sqlType = CodecUtil.getClickHouseDataType(nestedType);

            loadPsOnStack(methodVisitor);
            loadParameterIndexOnStack(methodVisitor);
            methodVisitor.visitVarInsn(ALOAD, currentLocalVarIndex++);

            String owner = Type.getInternalName(ClickHouseDataType.class);
            String value = sqlType.name();
            String descriptor = Type.getDescriptor(ClickHouseDataType.class);
            methodVisitor.visitFieldInsn(GETSTATIC, owner, value, descriptor);

            methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_ARRAY_NAME, CodecUtil.PROCESS_ARRAY_DESC, false);
            storeParameterIndexValue(methodVisitor);
        }

    }

    private void processSimpleType(MethodVisitor methodVisitor, String memberName,
                                   Class<?> memberDataType, String memberTypeDescriptor,
                                   DataTypes dbDataTypeValue) {
        if (IntrospectionType.BY_GETTERS == introspectionType)
            invokeCommonLoadGetterInstructions(methodVisitor, memberName, memberTypeDescriptor);
        else if (IntrospectionType.BY_FIELDS == introspectionType)
            invokeCommonLoadFieldInstructions(methodVisitor, memberName, memberTypeDescriptor);
        else
            throw unsupportedIntrospectionType();

        switch (dbDataTypeValue) {
            case ENUM8:
            case ENUM16:
                methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_ENUM_NAME, CodecUtil.PROCESS_ENUM_DESC, false);
                break;

            case STRING:
            case FIXED_STRING:
                methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_STRING_NAME, CodecUtil.PROCESS_STRING_DESC, false);
                break;

            case UINT8:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_BOOLEAN_NAME, CodecUtil.PROCESS_PRIMITIVE_BOOLEAN_DESC, false);
                else
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_BOOLEAN_NAME, CodecUtil.PROCESS_BOOLEAN_DESC, false);
                break;
            case UINT16:
            case UINT32:
            case UINT64:
                throw unsupportedDataTypeException(dbDataTypeValue, memberDataType);

            case INT8:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_BYTE_NAME, CodecUtil.PROCESS_PRIMITIVE_BYTE_DESC, false);
                else
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_BYTE_NAME, CodecUtil.PROCESS_BYTE_DESC, false);
                break;
            case INT16:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_SHORT_NAME, CodecUtil.PROCESS_PRIMITIVE_SHORT_DESC, false);
                else
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_SHORT_NAME, CodecUtil.PROCESS_SHORT_DESC, false);
                break;
            case INT32:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_INTEGER_NAME, CodecUtil.PROCESS_PRIMITIVE_INTEGER_DESC, false);
                else
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_INTEGER_NAME, CodecUtil.PROCESS_INTEGER_DESC, false);
                break;
            case INT64:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_LONG_NAME, CodecUtil.PROCESS_PRIMITIVE_LONG_DESC, false);
                else if (memberDataType.equals(Long.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_LONG_NAME, CodecUtil.PROCESS_LONG_DESC, false);
                else if (memberDataType.equals(Instant.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_INSTANT_AS_LONG_NAME, CodecUtil.PROCESS_INSTANT_DESC, false);
                else if (memberDataType.equals(Duration.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DURATION_AS_LONG_NAME, CodecUtil.PROCESS_DURATION_DESC, false);
                break;

            case FLOAT32:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_FLOAT_NAME, CodecUtil.PROCESS_PRIMITIVE_FLOAT_DESC, false);
                else
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_FLOAT_NAME, CodecUtil.PROCESS_FLOAT_DESC, false);
                break;
            case FLOAT64:
                if (memberDataType.isPrimitive())
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DOUBLE_NAME, CodecUtil.PROCESS_PRIMITIVE_DOUBLE_DESC, false);
                else
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DOUBLE_NAME, CodecUtil.PROCESS_DOUBLE_DESC, false);
                break;

            case DECIMAL:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                if (memberDataType.equals(Decimal64.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DECIMAL64_NAME, CodecUtil.PROCESS_DECIMAL64_DESC, false);
                else if (memberDataType.equals(BigDecimal.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_BIG_DECIMAL_NAME, CodecUtil.PROCESS_BIG_DECIMAL_DESC, false);
                break;

            case DATE:
                if (memberDataType.equals(long.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DATE_NAME, CodecUtil.PROCESS_DATE_BY_PRIMITIVE_LONG_DESC, false);
                else if (memberDataType.equals(Long.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DATE_NAME, CodecUtil.PROCESS_DATE_BY_LONG_DESC, false);
                else if (memberDataType.equals(LocalDate.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_DATE_NAME, CodecUtil.PROCESS_DATE_BY_LOCAL_DATE_DESC, false);
                break;

            case DATE_TIME:
            case DATE_TIME64:
                if (memberDataType.equals(Instant.class))
                    methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_INSTANT_NAME, CodecUtil.PROCESS_INSTANT_DESC, false);
                break;

            default:
                throw unsupportedDataTypeException(dbDataTypeValue, memberDataType);
        }
        storeParameterIndexValue(methodVisitor);
    }

    private void processArray(MethodVisitor methodVisitor, SchemaArrayElement elementAnnotation, String memberName,
                              Class<?> memberDataType, String memberTypeDescriptor) {
        Class<?> elementType;
        if (elementAnnotation != null)
            elementType = elementAnnotation.nestedType();
        else
            elementType = memberDataType.getComponentType();

        ClickHouseDataType sqlType = CodecUtil.getClickHouseDataType(elementType);

        loadPsOnStack(methodVisitor);
        loadParameterIndexOnStack(methodVisitor);
        loadMemberValueOnStack(methodVisitor, memberName, memberTypeDescriptor);
        if (memberDataType.isAssignableFrom(List.class))
            methodVisitor.visitMethodInsn(INVOKEINTERFACE, INTERNAL_LIST_NAME, "toArray", "()[Ljava/lang/Object;", true);

        String owner = Type.getInternalName(ClickHouseDataType.class);
        String value = sqlType.name();
        String descriptor = Type.getDescriptor(ClickHouseDataType.class);
        methodVisitor.visitFieldInsn(GETSTATIC, owner, value, descriptor);

        methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.PROCESS_ARRAY_NAME, CodecUtil.PROCESS_ARRAY_DESC, false);
        storeParameterIndexValue(methodVisitor);
    }

    private Object[] getLocalVarTypes(List<String> arrayDescriptors, int numLocal) {
        Object[] types = new Object[numLocal];

        int index = 0;
        types[index++] = generatedClassName;
        types[index++] = "java/lang/Object";
        types[index++] = INTERNAL_PS_NAME;
        types[index++] = internalClassName;
        types[index++] = Opcodes.INTEGER;
        types[index++] = INTERNAL_LIST_NAME;
        types[index++] = Opcodes.INTEGER;
        for (String item : arrayDescriptors) {
            types[index++] = item;
        }
        types[index] = Opcodes.INTEGER;

        return types;
    }

    private int initArray(MethodVisitor methodVisitor, Class<?> nestedType,
                          String nestedInternalName, int currentLocalVarIndex) {
        loadSizeOfNestedMemberValueOnStack(methodVisitor);

        if (!nestedType.isPrimitive()) {
            // invoke instruction for initializing reference type array
            methodVisitor.visitTypeInsn(ANEWARRAY, nestedInternalName);
        } else if (nestedType.equals(long.class)) {
            // invoke instruction for initializing long array
            methodVisitor.visitIntInsn(NEWARRAY, T_LONG);
        } else if (nestedType.equals(int.class)) {
            // invoke instruction for initializing int array
            methodVisitor.visitIntInsn(NEWARRAY, T_INT);
        } else if (nestedType.equals(short.class)) {
            // invoke instruction for initializing short array
            methodVisitor.visitIntInsn(NEWARRAY, T_SHORT);
        } else if (nestedType.equals(byte.class)) {
            // invoke instruction for initializing byte array
            methodVisitor.visitIntInsn(NEWARRAY, T_BYTE);
        } else if (nestedType.equals(float.class)) {
            // invoke instruction for initializing float array
            methodVisitor.visitIntInsn(NEWARRAY, T_FLOAT);
        } else if (nestedType.equals(double.class)) {
            // invoke instruction for initializing double array
            methodVisitor.visitIntInsn(NEWARRAY, T_DOUBLE);
        } else if (nestedType.equals(boolean.class)) {
            // invoke instruction for initializing boolean array
            methodVisitor.visitIntInsn(NEWARRAY, T_BOOLEAN);
        }
        // store an array into the local variable #'currentLocalVarIndex'
        methodVisitor.visitVarInsn(ASTORE, currentLocalVarIndex);

        localVarSize++;
        return ++currentLocalVarIndex;
    }

    private void invokeCommonLoadFieldInstructions(MethodVisitor methodVisitor,
                                                   String memberName,
                                                   String memberTypeDescriptor) {
        loadPsOnStack(methodVisitor);
        loadParameterIndexOnStack(methodVisitor);
        loadFieldValueOnStack(methodVisitor, memberName, memberTypeDescriptor);
    }

    private void invokeCommonLoadGetterInstructions(MethodVisitor methodVisitor,
                                                    String memberName,
                                                    String memberTypeDescriptor) {
        loadPsOnStack(methodVisitor);
        loadParameterIndexOnStack(methodVisitor);
        loadFieldValueByGetterOnStack(methodVisitor, memberName, memberTypeDescriptor);
    }

    private void loadPsOnStack(MethodVisitor methodVisitor) {
        // load `PreparedStatement` onto the stack from a local variable #2
        methodVisitor.visitVarInsn(ALOAD, 2);
    }

    private void loadParameterIndexOnStack(MethodVisitor methodVisitor) {
        // load an int value from a local variable #4 (parameterIndex)
        methodVisitor.visitVarInsn(ILOAD, 4);
    }

    private void storeParameterIndexValue(MethodVisitor methodVisitor) {
        // store an int value into the local variable #4 (parameterIndex)
        methodVisitor.visitVarInsn(ISTORE, 4);
        localVarSize++;
    }

    private void loadCurrentModelOnStack(MethodVisitor methodVisitor) {
        // load a reference onto the stack from a local variable #3
        methodVisitor.visitVarInsn(ALOAD, 3);
    }

    private void loadFieldValueOnStack(MethodVisitor methodVisitor, String memberName, String memberTypeDescriptor) {
        loadCurrentModelOnStack(methodVisitor);
        // load field value onto the stack
        loadOnlyValueOnStack(methodVisitor, internalClassName, memberName, memberTypeDescriptor);
    }

    private void loadFieldValueByGetterOnStack(MethodVisitor methodVisitor, String memberName, String memberTypeDescriptor) {
        loadCurrentModelOnStack(methodVisitor);
        // load field value invoking get method onto the stack
        loadOnlyValueByGetterOnStack(methodVisitor, internalClassName, memberName, memberTypeDescriptor);
    }

    private void loadMemberValueOnStack(MethodVisitor methodVisitor, String memberName, String memberTypeDescriptor) {
        if (IntrospectionType.BY_GETTERS == introspectionType)
            loadFieldValueByGetterOnStack(methodVisitor, memberName, memberTypeDescriptor);
        else
            loadFieldValueOnStack(methodVisitor, memberName, memberTypeDescriptor);
    }

    private void loadOnlyValueOnStack(MethodVisitor methodVisitor, String memberClassName, String memberName, String memberTypeDescriptor) {
        methodVisitor.visitFieldInsn(GETFIELD, memberClassName, memberName, memberTypeDescriptor);
    }

    private void loadOnlyValueByGetterOnStack(MethodVisitor methodVisitor, String memberClassName, String memberName, String memberTypeDescriptor) {
        methodVisitor.visitMethodInsn(INVOKEVIRTUAL, memberClassName, memberName, memberTypeDescriptor, false);
    }

    private void loadNestedMemberValueOnStack(MethodVisitor methodVisitor, String memberClassName, String memberName, String memberTypeDescriptor) {
        if (IntrospectionType.BY_GETTERS == introspectionType)
            loadOnlyValueByGetterOnStack(methodVisitor, memberClassName, memberName, memberTypeDescriptor);
        else
            loadOnlyValueOnStack(methodVisitor, memberClassName, memberName, memberTypeDescriptor);
    }

    private void storeNestedMemberValue(MethodVisitor methodVisitor) {
        // store nested field in local variable #5
        methodVisitor.visitVarInsn(ASTORE, 5);
        localVarSize++;
    }

    private void loadNestedMemberOnStack(MethodVisitor methodVisitor) {
        // load nested field value onto the stack from a local variable #5
        methodVisitor.visitVarInsn(ALOAD, 5);
    }

    private void storeSizeOfNestedMemberValue(MethodVisitor methodVisitor) {
        // store size of nested field value in local variable #6
        methodVisitor.visitVarInsn(ISTORE, 6);
        localVarSize++;
    }

    private void loadSizeOfNestedMemberValueOnStack(MethodVisitor methodVisitor) {
        // load size of nested field value onto the stack from a local variable #6
        methodVisitor.visitVarInsn(ILOAD, 6);
    }

    private void invokeMemberValueValidation(MethodVisitor methodVisitor, String memberName, String memberTypeDescriptor) {
        loadMemberValueOnStack(methodVisitor, memberName, memberTypeDescriptor);
        // load `memberName` onto the stack.
        methodVisitor.visitLdcInsn(memberName);
        // invoke `validateValue` static method in CodecUtil class.
        methodVisitor.visitMethodInsn(INVOKESTATIC, CodecUtil.INTERNAL_NAME, CodecUtil.VALIDATE_VALUE_FOR_NULL_NAME, CodecUtil.VALIDATE_VALUE_FOR_NULL_DESC, false);
    }

    private static RuntimeException unsupportedDataTypeException(DataTypes dataType, Class<?> currentType) {
        throw new IllegalArgumentException(String.format("Unsupported `%s` data type for class `%s`.", dataType, currentType.getName()));
    }

    private RuntimeException unsupportedIntrospectionType() {
        throw new UnsupportedOperationException(String.format("Unknown introspection type '%s'", introspectionType));
    }
}