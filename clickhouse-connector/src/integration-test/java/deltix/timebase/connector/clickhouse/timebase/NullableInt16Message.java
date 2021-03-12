package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableInt16Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableInt16Message.class.getName();

    private short nullableInt16Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT16,
            dataType = SchemaDataType.INTEGER
    )
    public short getNullableInt16Field() {
        return nullableInt16Field;
    }

    public void setNullableInt16Field(short nullableInt16Field) {
        this.nullableInt16Field = nullableInt16Field;
    }
}
