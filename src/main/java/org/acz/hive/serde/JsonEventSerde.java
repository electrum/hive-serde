package org.acz.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.codehaus.jackson.JsonNode;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Properties;

public class JsonEventSerde
        extends JsonSerde
{
    public static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter HIVE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd' 'HH:mm:ss").withZone(DateTimeZone.UTC);
    private Integer timestampColumn;

    @Override
    public void initialize(Configuration configuration, Properties table)
            throws SerDeException
    {
        super.initialize(configuration, table);

        timestampColumn = columnNameMap.get("ts");
    }

    @Override
    protected Object[] buildStruct(JsonNode tree)
            throws IOException, SerDeException
    {
        if (!tree.has("data")) {
            throw new IOException("data field is missing");
        }
        JsonNode dataNode = tree.get("data");
        if (!dataNode.isObject()) {
            throw new IOException("data field is not an object");
        }

        Object[] struct = processFields(dataNode);

        if (timestampColumn != null) {
            struct[timestampColumn] = HIVE_FORMATTER.print(parseTimestamp(tree));
        }

        return struct;
    }

    private static long parseTimestamp(JsonNode tree)
            throws IOException
    {
        if (!tree.has("timestamp")) {
            throw new IOException("timestamp field is missing");
        }
        JsonNode node = tree.get("timestamp");
        if (!node.isTextual()) {
            throw new IOException("timestamp field is not text");
        }
        String timestamp = node.getTextValue();
        try {
            return ISO_FORMATTER.parseMillis(timestamp);
        }
        catch (Exception e) {
            throw new IOException("invalid timestamp: " + timestamp);
        }
    }
}
