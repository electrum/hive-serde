package org.acz.hive.serde;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.testng.annotations.Test;

import java.util.Map;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCaseInsensitiveMap
{
    @Test
    public void testMap()
    {
        Map<String, String> map = new CaseInsensitiveMap<String>();
        map.put("HELLO", "world");

        assertEquals(map.get("hello"), "world");
        assertEquals(map.get("HELLO"), "world");
        assertEquals(map.get("Hello"), "world");

        assertTrue(map.containsKey("hello"));
        assertTrue(map.containsKey("HELLO"));
        assertTrue(map.containsKey("Hello"));

        assertNull(map.get("foo"));
        assertFalse(map.containsKey("foo"));
    }

    @Test
    public void testObjectMapper()
            throws Exception
    {
        SimpleModule module = new SimpleModule("module", Version.unknownVersion());
        module.addAbstractTypeMapping(Map.class, CaseInsensitiveMap.class);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);

        Map map = objectMapper.readValue("{\"HELLO\":\"world\"}", Map.class);

        assertTrue(map instanceof CaseInsensitiveMap);

        assertInstanceOf(map, CaseInsensitiveMap.class);

        assertEquals(map.get("hello"), "world");
        assertEquals(map.get("HELLO"), "world");
        assertEquals(map.get("Hello"), "world");

        assertTrue(map.containsKey("hello"));
        assertTrue(map.containsKey("HELLO"));
        assertTrue(map.containsKey("Hello"));

        assertNull(map.get("foo"));
        assertFalse(map.containsKey("foo"));
    }

    private static void assertInstanceOf(Object actual, Class<?> expectedType)
    {
        if (!expectedType.isInstance(actual)) {
            fail(format("expected %s to be an instance of %s", actual.getClass().getName(), expectedType.getName()));
        }
    }
}
