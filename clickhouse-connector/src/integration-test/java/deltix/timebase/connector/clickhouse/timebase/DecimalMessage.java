package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class DecimalMessage extends InstrumentMessage {
    public static final String CLASS_NAME = DecimalMessage.class.getName();

    private double decimalField;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_SCALE_AUTO,
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public double getDecimalField() {
        return decimalField;
    }

    public void setDecimalField(double decimalField) {
        this.decimalField = decimalField;
    }
}
