package org.acz.hive.serde;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CaseInsensitiveMap<V>
        implements Map<String, V>
{
    Map<String, V> map = new HashMap<String, V>();

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return map.containsKey(convertKey(key));
    }

    @Override
    public boolean containsValue(Object value)
    {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key)
    {
        return map.get(convertKey(key));
    }

    @Override
    public V remove(Object key)
    {
        return map.remove(convertKey(key));
    }

    @Override
    public void clear()
    {
        map.clear();
    }

    @Override
    public Set<String> keySet()
    {
        throw new UnsupportedOperationException("keySet not supported");
    }

    @Override
    public Collection<V> values()
    {
        return map.values();
    }

    @Override
    public Set<Entry<String, V>> entrySet()
    {
        throw new UnsupportedOperationException("entrySet not supported");
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m)
    {
        for (Entry<? extends String, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public V put(String key, V value)
    {
        return map.put(convertKey(key), value);
    }

    private static String convertKey(Object key)
    {
        return ((String) key).toLowerCase();
    }
}
