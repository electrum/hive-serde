package org.acz.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;

public class JsonEventSerde
        implements SerDe
{
    public static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter HIVE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd' 'HH:mm:ss").withZone(DateTimeZone.UTC);

    private final ObjectMapper objectMapper;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;
    private ObjectInspector rowObjectInspector;
    private Map<String, Integer> columnNameMap;
    private Integer timestampColumn;

    public JsonEventSerde()
    {
        SimpleModule module = new SimpleModule("module", Version.unknownVersion());
        module.addAbstractTypeMapping(Map.class, CaseInsensitiveMap.class);
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
    }

    @Override
    public void initialize(Configuration configuration, Properties table)
            throws SerDeException
    {
        String columnNamesProperty = table.getProperty(Constants.LIST_COLUMNS);
        if ((columnNamesProperty == null) || columnNamesProperty.isEmpty()) {
            throw new SerDeException("table has no columns");
        }
        String columnTypesProperty = table.getProperty(Constants.LIST_COLUMN_TYPES);
        if ((columnTypesProperty == null) || columnTypesProperty.isEmpty()) {
            throw new SerDeException("table has no column types");
        }

        columnNames = asList(columnNamesProperty.toLowerCase().split(","));
        columnTypes = getTypeInfosFromTypeString(columnTypesProperty);
        if (columnNames.size() != columnTypes.size()) {
            throw new SerDeException(format("columns size (%s) does not match column types size (%s)", columnNames.size(), columnTypes.size()));
        }

        rowObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(getStructTypeInfo(columnNames, columnTypes));

        columnNameMap = mapColumns(columnNames);
        timestampColumn = columnNameMap.get("ts");
    }

    private static Map<String, Integer> mapColumns(List<String> columnNames)
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i), i);
        }
        return map;
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        throw new UnsupportedOperationException("serialization not supported");
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector)
            throws SerDeException
    {
        throw new UnsupportedOperationException("serialization not supported");
    }

    @Override
    public Object deserialize(Writable writable)
            throws SerDeException
    {
        if (!(writable instanceof BinaryComparable)) {
            throw new SerDeException("expected BinaryComparable: " + writable.getClass().getName());
        }
        BinaryComparable binary = (BinaryComparable) writable;

        try {
            JsonParser jsonParser = objectMapper.getJsonFactory().createJsonParser(binary.getBytes(), 0, binary.getLength());
            return buildStruct(objectMapper.readTree(jsonParser));
        }
        catch (IOException e) {
            throw new SerDeException("error parsing JSON", e);
        }
    }

    @Override
    public ObjectInspector getObjectInspector()
            throws SerDeException
    {
        return rowObjectInspector;
    }

    private Object[] buildStruct(JsonNode tree)
            throws IOException, SerDeException
    {
        Object[] struct = new Object[columnNames.size()];

        if (!tree.has("data")) {
            throw new IOException("data field is missing");
        }
        JsonNode dataNode = tree.get("data");
        if (!dataNode.isObject()) {
            throw new IOException("data field is not an object");
        }

        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.getFields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey().toLowerCase();
            JsonNode node = entry.getValue();

            Integer columnIndex = columnNameMap.get(key);
            if (columnIndex != null) {
                struct[columnIndex] = getNodeValue(node, key, columnTypes.get(columnIndex));
            }
        }

        if (timestampColumn != null) {
            struct[timestampColumn] = HIVE_FORMATTER.print(parseTimestamp(tree));
        }

        return struct;
    }

    private Object getNodeValue(JsonNode node, String columnName, TypeInfo typeInfo)
            throws IOException, SerDeException
    {
        switch (typeInfo.getCategory()) {
            case LIST:
                return objectMapper.readValue(node, List.class);
            case MAP:
                return objectMapper.readValue(node, Map.class);
            case PRIMITIVE:
                PrimitiveTypeInfo ptypeInfo = (PrimitiveTypeInfo) typeInfo;
                switch (ptypeInfo.getPrimitiveCategory()) {
                    case VOID:
                        throw new SerDeException("cannot deserialize to VOID type for column " + columnName);
                    case UNKNOWN:
                        throw new SerDeException("cannot deserialize to UNKNOWN type for column " + columnName);
                    case BOOLEAN:
                        return node.getBooleanValue();
                    case BYTE:
                        return (byte) node.getIntValue();
                    case SHORT:
                        return (short) node.getIntValue();
                    case INT:
                        return node.getIntValue();
                    case LONG:
                        return node.getLongValue();
                    case FLOAT:
                        return (float) node.getDoubleValue();
                    case DOUBLE:
                        return node.getDoubleValue();
                    case STRING:
                        return node.getTextValue();
                    default:
                        throw new SerDeException("unhandled primitive type: " + ptypeInfo.getPrimitiveCategory());
                }
            default:
                throw new SerDeException(format("unexpected type category (%s) for column: %s", typeInfo.getCategory(), columnName));
        }
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
