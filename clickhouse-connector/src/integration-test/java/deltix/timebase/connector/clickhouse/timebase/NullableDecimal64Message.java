package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class NullableDecimal64Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableDecimal64Message.class.getName();

    private long decimal64Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_DECIMAL64,
            dataType = SchemaDataType.FLOAT
    )
    public long getDecimal64Field() {
        return decimal64Field;
    }

    public void setDecimal64Field(long decimal64Field) {
        this.decimal64Field = decimal64Field;
    }
}
