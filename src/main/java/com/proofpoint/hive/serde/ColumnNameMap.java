/*
 * Copyright 2011 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
