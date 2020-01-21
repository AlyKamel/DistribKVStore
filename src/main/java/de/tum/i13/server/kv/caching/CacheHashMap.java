package de.tum.i13.server.kv.caching;

// source: https://stackoverflow.com/questions/5911174/finding-key-associated-with-max-value-in-a-java-map

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * CacheHashMap is an extension to the LinkedHashMap in order to provide support for the LFU caching
 * strategy. It does that by making use of a HashMap to modify the amount of times a key was
 * requested.
 */
@SuppressWarnings("serial")
public class CacheHashMap extends LinkedHashMap<String, String> {

  private final int capacity;
  private final HashMap<String, Integer> usage;
  private final boolean lfu;
  private String minKey;

  public CacheHashMap(CachingStrategy strategy, int capacity) {
    super(capacity, .75f, strategy == CachingStrategy.LRU);
    this.capacity = capacity;
    usage = new HashMap<String, Integer>();
    lfu = strategy == CachingStrategy.LFU;
  }

  @Override
  public String put(String key, String value) {
    incrementUsage(key);
    if (size() >= capacity) {
      minKey = Collections.min(usage.entrySet(), Map.Entry.comparingByValue()).getKey();
    }
    usage.computeIfAbsent(key, k -> 0);
    return super.put(key, value);
  }

  @Override
  public String get(Object key) {
    incrementUsage((String) key);
    return super.get(key);
  }

  @Override
  public String remove(Object key) {
    usage.remove(key);
    return super.remove(key);
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
    boolean full = size() > capacity;
    if (full && lfu) {
      remove(minKey);
      return false;
    }
    return full;
  }
  
  private void incrementUsage(String key) {
    usage.computeIfPresent(key, (k, v) -> v + 1);
  }

}
