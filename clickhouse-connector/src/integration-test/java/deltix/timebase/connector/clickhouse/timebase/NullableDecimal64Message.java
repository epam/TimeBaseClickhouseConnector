package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import com.epam.deltix.timebase.messages.*;

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
