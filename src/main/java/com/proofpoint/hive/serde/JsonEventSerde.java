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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.codehaus.jackson.JsonNode;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Map;
import java.util.Properties;

public class JsonEventSerde
        extends JsonSerde
{
    public static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter HIVE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd' 'HH:mm:ss").withZone(DateTimeZone.UTC);
    private Integer uuidColumn;
    private Integer hostColumn;
    private Integer timestampColumn;

    @Override
    public void initialize(Configuration configuration, Properties table)
            throws SerDeException
    {
        super.initialize(configuration, table);

        Map<String, Integer> columnNames = columnNameMap.getColumnNames(rootTypeInfo);
        uuidColumn = columnNames.get("uuid");
        hostColumn = columnNames.get("host");
        timestampColumn = columnNames.get("ts");
    }

    @Override
    protected Object[] buildStruct(JsonNode tree)
            throws SerDeException
    {
        if (!tree.has("data")) {
            throw new SerDeException("data field is missing");
        }
        JsonNode dataNode = tree.get("data");
        if (!dataNode.isObject()) {
            throw new SerDeException("data field is not an object");
        }

        Object[] struct = processFields(dataNode);

        if (uuidColumn != null) {
            struct[uuidColumn] = getTextNode(tree, "uuid");
        }
        if (hostColumn != null) {
            struct[hostColumn] = getTextNode(tree, "host");
        }
        if (timestampColumn != null) {
            long ts = parseTimestamp(getTextNode(tree, "timestamp"));
            struct[timestampColumn] = HIVE_FORMATTER.print(ts);
        }

        return struct;
    }

    private static long parseTimestamp(String timestamp)
            throws SerDeException
    {
        try {
            return ISO_FORMATTER.parseMillis(timestamp);
        }
        catch (Exception e) {
            throw new SerDeException("invalid timestamp: " + timestamp);
        }
    }

    private static String getTextNode(JsonNode tree, String field)
            throws SerDeException
    {
        if (!tree.has(field)) {
            throw new SerDeException(field + " field is missing");
        }
        JsonNode node = tree.get(field);
        if (!node.isTextual()) {
            throw new SerDeException(field + " field is not text");
        }
        return node.getTextValue();
    }
}
