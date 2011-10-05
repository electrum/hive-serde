package org.acz.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;

public class JsonSerde
        implements SerDe
{
    private final JsonFactory jsonFactory = new ObjectMapper().getJsonFactory();
    private ObjectInspector rowObjectInspector;
    protected StructTypeInfo rootTypeInfo;
    protected ColumnNameMap columnNameMap;

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

        List<String> columnNames = asList(columnNamesProperty.split(","));
        List<TypeInfo> columnTypes = getTypeInfosFromTypeString(columnTypesProperty);
        if (columnNames.size() != columnTypes.size()) {
            throw new SerDeException(format("columns size (%s) does not match column types size (%s)", columnNames.size(), columnTypes.size()));
        }

        rootTypeInfo = (StructTypeInfo) getStructTypeInfo(columnNames, columnTypes);
        rowObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(rootTypeInfo);

        columnNameMap = new ColumnNameMap(rootTypeInfo);
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
            JsonParser jsonParser = jsonFactory.createJsonParser(binary.getBytes(), 0, binary.getLength());
            return buildStruct(jsonParser.readValueAsTree());
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

    protected Object buildStruct(JsonNode tree)
            throws SerDeException
    {
        return processFields(tree);
    }

    protected Object[] processFields(JsonNode tree)
            throws SerDeException
    {
        return getStructNodeValue(null, tree, rootTypeInfo);
    }

    private Object getNodeValue(String columnName, JsonNode node, TypeInfo typeInfo)
            throws SerDeException
    {
        if (node.isNull()) {
            return null;
        }
        switch (typeInfo.getCategory()) {
            case LIST:
                return getListNodeValue(columnName, node, (ListTypeInfo) typeInfo);
            case MAP:
                return getMapNodeValue(columnName, node, (MapTypeInfo) typeInfo);
            case PRIMITIVE:
                return getPrimitiveNodeValue(columnName, node, (PrimitiveTypeInfo) typeInfo);
            case STRUCT:
                return getStructNodeValue(columnName, node, (StructTypeInfo) typeInfo);
            default:
                throw new SerDeException(format("unexpected type category (%s) for column: %s", typeInfo.getCategory(), columnName));
        }
    }

    private Object getListNodeValue(String columnName, JsonNode node, ListTypeInfo typeInfo)
            throws SerDeException
    {
        if (!node.isArray()) {
            throw new SerDeException(format("expected list, found %s for column %s", node.getClass().getSimpleName(), columnName));
        }
        List<Object> list = new ArrayList<Object>(node.size());
        for (JsonNode item : node) {
            list.add(getNodeValue(columnName, item, typeInfo.getListElementTypeInfo()));
        }
        return list;
    }

    private Object getMapNodeValue(String columnName, JsonNode node, MapTypeInfo typeInfo)
            throws SerDeException
    {
        if (!node.isObject()) {
            throw new SerDeException(format("expected map, found %s for column %s", node.getClass().getSimpleName(), columnName));
        }
        if (typeInfo.getMapKeyTypeInfo().getCategory() != Category.PRIMITIVE) {
            throw new SerDeException("map key is not a primitive: " + typeInfo.getMapKeyTypeInfo());
        }
        PrimitiveTypeInfo keyType = (PrimitiveTypeInfo) typeInfo.getMapKeyTypeInfo();
        if (keyType.getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new SerDeException(format("expected STRING map key, found %s for column %s", keyType.getPrimitiveCategory(), columnName));
        }

        Map<String, Object> map = new CaseInsensitiveMap<Object>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.getFields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            Object object = getNodeValue(columnName, value, typeInfo.getMapValueTypeInfo());
            if (map.put(key, object) != null) {
                throw new SerDeException(format("column %s case-insensitive map already contains key: %s", columnName, key));
            }
        }
        return map;
    }

    private Object getPrimitiveNodeValue(String columnName, JsonNode node, PrimitiveTypeInfo typeInfo)
            throws SerDeException
    {
        if (!node.isValueNode()) {
            throw new SerDeException(format("expected primitive, found %s for column %s", node.getClass().getSimpleName(), columnName));
        }
        switch (typeInfo.getPrimitiveCategory()) {
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
                throw new SerDeException("unhandled primitive type: " + typeInfo.getPrimitiveCategory());
        }
    }

    private Object[] getStructNodeValue(String columnName, JsonNode node, StructTypeInfo typeInfo)
            throws SerDeException
    {
        Map<String, Integer> columnNames = columnNameMap.getColumnNames(typeInfo);
        List<TypeInfo> fieldTypes = typeInfo.getAllStructFieldTypeInfos();

        Object[] struct = new Object[fieldTypes.size()];
        Iterator<Map.Entry<String, JsonNode>> fields = node.getFields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey().toLowerCase();
            JsonNode value = entry.getValue();

            Integer columnIndex = columnNames.get(key);
            if (columnIndex != null) {
                String structColumn = (columnName == null) ? key : (columnName + "." + key);
                struct[columnIndex] = getNodeValue(structColumn, value, fieldTypes.get(columnIndex));
            }
        }
        return struct;
    }
}
