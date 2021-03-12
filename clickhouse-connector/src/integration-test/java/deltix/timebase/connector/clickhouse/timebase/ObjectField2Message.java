package deltix.timebase.connector.clickhouse.timebase;

//public class ObjectField2Message extends InstrumentMessage {
//    public static final String CLASS_NAME = ObjectField2Message.class.getName();
//
//    private String stringField;
//    private ObjectFieldMessage objectField;
//
//
//    @SchemaElement
//    @SchemaType(isNullable = false)
//    public String getStringField() {
//        return stringField;
//    }
//
//    public void setStringField(String stringField) {
//        this.stringField = stringField;
//    }
//
//    @SchemaElement
//    @SchemaType(isNullable = true)
//    public ObjectFieldMessage getObjectField() {
//        return objectField;
//    }
//
//    public void setObjectField(ObjectFieldMessage objectField) {
//        this.objectField = objectField;
//    }
//}

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.RecordInfo;

@deltix.timebase.messages.SchemaElement( name="deltix.timebase.connector.clickhouse.timebase.ObjectField2Message" )
public class ObjectField2Message extends InstrumentMessage {
    /**
     *
     */
    @deltix.timebase.messages.SchemaType( dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = { ObjectFieldMessage.class  })
    public InstrumentMessage objectField;
    /**
     *
     */
    public String stringField;
    @java.lang.Override
    public InstrumentMessage copyFrom (RecordInfo source)
    {
        super.copyFrom (source);
        if (source instanceof ObjectField2Message)
        {
            final ObjectField2Message obj = (ObjectField2Message)source;
            objectField = obj.objectField;
            stringField = obj.stringField;
        }
        return this;
    }
    @java.lang.Override
    public String toString ()
    {
        return super.toString ()+", "+"objectField"+": "+objectField+", "+"stringField"+": "+stringField;
    }
    @java.lang.Override
    public InstrumentMessage clone ()
    {
        final ObjectField2Message msg = new ObjectField2Message ();
        msg.copyFrom (this);
        return msg;
    }

    @deltix.timebase.messages.SchemaType( dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = { ObjectFieldMessage.class  })
    public InstrumentMessage getObjectField() {
        return objectField;
    }

    public void setObjectField(InstrumentMessage objectField) {
        this.objectField = objectField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }
}
