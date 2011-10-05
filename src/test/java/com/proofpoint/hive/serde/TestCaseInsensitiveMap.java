package com.proofpoint.hive.serde;

import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
}
