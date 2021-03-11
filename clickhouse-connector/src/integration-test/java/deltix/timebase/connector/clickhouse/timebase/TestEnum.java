package deltix.timebase.connector.clickhouse.timebase;


import deltix.timebase.messages.SchemaElement;

@SchemaElement(
        name = "deltix.timebase.connector.clickhouse.timebase.TestEnum"
)
public enum TestEnum {
    @SchemaElement
    FIRST,
    @SchemaElement
    SECOND,
    @SchemaElement
    DEFAULT
}
