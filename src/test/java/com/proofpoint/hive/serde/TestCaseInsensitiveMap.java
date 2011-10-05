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
