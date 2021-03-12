package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableInt32Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableInt32Message.class.getName();

    private int nullableInt32Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT32,
            dataType = SchemaDataType.INTEGER
    )
    public int getNullableInt32Field() {
        return nullableInt32Field;
    }

    public void setNullableInt32Field(int nullableInt32Field) {
        this.nullableInt32Field = nullableInt32Field;
    }
}
