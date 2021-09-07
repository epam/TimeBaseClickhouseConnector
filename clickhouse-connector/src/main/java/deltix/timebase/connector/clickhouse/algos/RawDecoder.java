package deltix.timebase.connector.clickhouse.algos;

import deltix.clickhouse.schema.types.Int16DataType;
import deltix.clickhouse.schema.types.Int32DataType;
import deltix.clickhouse.schema.types.Int64DataType;
import deltix.clickhouse.schema.types.Int8DataType;
import com.epam.deltix.dfp.Decimal64Utils;
import com.epam.deltix.qsrv.hf.pub.NullValueException;
import com.epam.deltix.qsrv.hf.pub.ReadableValue;
import com.epam.deltix.qsrv.hf.pub.codec.NonStaticFieldInfo;
import com.epam.deltix.qsrv.hf.pub.codec.UnboundDecoder;
import com.epam.deltix.qsrv.hf.pub.md.*;

import java.math.BigDecimal;
import java.util.*;

import static com.epam.deltix.qsrv.hf.pub.MessageUtils.OBJECT_CLASS_NAME;

public class RawDecoder {

    public static Object readField(DataType type, ReadableValue rv) {
        try {
            if (type instanceof IntegerDataType) {
                return readInteger((IntegerDataType) type, rv);

            } else if (type instanceof FloatDataType)
                return readFloat((FloatDataType) type, rv);

            else if (type instanceof CharDataType)
                return rv.getChar();
            else if (type instanceof EnumDataType || type instanceof VarcharDataType)
                return rv.getString();
            else if (type instanceof BooleanDataType)
                return readBoolean((BooleanDataType)type, rv);

            else if (type instanceof DateTimeDataType)
                return readDateTime((DateTimeDataType)type, rv);
            else if (type instanceof TimeOfDayDataType)
                return rv.getInt();
            else if (type instanceof ArrayDataType)
                return readArray((ArrayDataType) type, rv);
            else if (type instanceof ClassDataType)
                return readObjectValues(rv);
            else if (type instanceof BinaryDataType) {
                try {
                    final int size = rv.getBinaryLength();
                    final byte[] bin = new byte[size];
                    rv.getBinary(0, size, bin, 0);
                    return bin;
                } catch (NullValueException e) {
                    return null;
                }
            } else
                throw new RuntimeException("Unrecognized dataType: " + type);
        } catch (NullValueException e) {
            return null;
        }
    }

    protected static Object    readBoolean(BooleanDataType type, ReadableValue rv) {
       return rv.getBoolean();
    }

    protected static Object    readInteger(IntegerDataType tbIntegerDataType, ReadableValue rv) {

        if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT8))
            return (byte) rv.getInt();
        else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT16))
            return (short) rv.getInt();
        else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT32))
            return rv.getInt();
        else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT64))
            return rv.getLong();
        else
            throw new UnsupportedOperationException(String.format("Unexpected encoding %s for IntegerDataType", tbIntegerDataType.getEncoding()));
//        int size = type.getNativeTypeSize();
//
//        if (size >= 6)
//            return rv.getLong();
//        else if (size == 1)
//            return (byte) rv.getInt();
//        else if (size == 2)
//            return (short) rv.getInt();
//        else
//            return rv.getInt();
    }

    protected static Object    readFloat(FloatDataType tbFloatDataType, ReadableValue rv) {
        if (tbFloatDataType.getScale() == FloatDataType.FIXED_FLOAT) // or encoding?
            return rv.getFloat();
        else if (tbFloatDataType.getScale() == FloatDataType.FIXED_DOUBLE)
            return  rv.getDouble();
        else if (tbFloatDataType.getScale() == FloatDataType.SCALE_AUTO ||
                tbFloatDataType.getScale() == FloatDataType.SCALE_DECIMAL64)
            return  new BigDecimal(Decimal64Utils.toString(rv.getLong()));
        else
            return  rv.getDouble();
    }

    protected static Object    readDateTime(DateTimeDataType type, ReadableValue rv) {
        return rv.getLong();
    }

    private static Object[]    readArray(ArrayDataType type, ReadableValue udec) throws NullValueException {
        final int len = udec.getArrayLength();
        final DataType elementType = type.getElementDataType();

        final boolean isNullableBool = (elementType instanceof BooleanDataType) && elementType.isNullable();

        Object[] values = new Object[len];

        for (int i = 0; i < len; i++) {
            Object value;
            try {
                final ReadableValue rv = udec.nextReadableElement();
                value = readField(elementType, rv);
            } catch (NullValueException e) {
                value = null;
            }

            if (isNullableBool) {
                Boolean b = (Boolean) value;
                values[i] = (byte) (b == null ? -1 : b ? 1 : 0);
            } else {
                values[i] = value;
            }

        }
        return values;
    }

    private static Map<String, Object> readObjectValues(ReadableValue udec) throws NullValueException {

        final UnboundDecoder decoder = udec.getFieldDecoder();
        Map<String, Object> values = new LinkedHashMap<>();

        if (decoder.getClassInfo() != null)
            values.put(OBJECT_CLASS_NAME, decoder.getClassInfo().getDescriptor());

        // dump field/value pairs
        while (decoder.nextField()) {
            NonStaticFieldInfo field = decoder.getField();
            Object value = readField(field.getType(), decoder);
            values.put(field.getName(), value);
        }

        return values;
    }
}
