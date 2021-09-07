package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableInt64Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableInt64Message.class.getName();

    private long nullableInt64Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER
    )
    public long getNullableInt64Field() {
        return nullableInt64Field;
    }

    public void setNullableInt64Field(long nullableInt64Field) {
        this.nullableInt64Field = nullableInt64Field;
    }
}
