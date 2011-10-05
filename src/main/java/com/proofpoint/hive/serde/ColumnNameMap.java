package com.proofpoint.hive.serde;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class ColumnNameMap
{
    // use identity because equals is very slow and TypeInfoFactory guarantees only one instance
    Map<StructTypeInfo, Map<String, Integer>> types = new IdentityHashMap<StructTypeInfo, Map<String, Integer>>();

    public ColumnNameMap(StructTypeInfo typeInfo)
    {
        process(typeInfo);
    }

    public Map<String, Integer> getColumnNames(StructTypeInfo typeInfo)
            throws SerDeException
    {
        Map<String, Integer> map = types.get(typeInfo);
        if (map == null) {
            throw new SerDeException("no column name map for type: "  + typeInfo);
        }
        return map;
    }

    private void process(TypeInfo typeInfo)
    {
        if (typeInfo.getCategory() == ObjectInspector.Category.LIST) {
            process(((ListTypeInfo) typeInfo).getListElementTypeInfo());
        }
        else if (typeInfo.getCategory() == ObjectInspector.Category.MAP) {
            process(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
        }
        else if (typeInfo.getCategory() == ObjectInspector.Category.STRUCT) {
            process((StructTypeInfo) typeInfo);
        }
    }

    private void process(StructTypeInfo structTypeInfo)
    {
        types.put(structTypeInfo, mapColumns(structTypeInfo.getAllStructFieldNames()));
        for (TypeInfo fieldTypeInfo : structTypeInfo.getAllStructFieldTypeInfos()) {
            process(fieldTypeInfo);
        }
    }

    private static Map<String, Integer> mapColumns(List<String> columnNames)
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i).toLowerCase(), i);
        }
        return map;
    }
}
