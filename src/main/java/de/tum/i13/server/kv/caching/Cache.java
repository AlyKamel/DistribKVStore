package de.tum.i13.server.kv.caching;

import de.tum.i13.server.kv.KVStorageSystem;
import de.tum.i13.server.kv.ServerStatus;

public class Cache implements KVStorageSystem {

  private final CacheHashMap map;

  public Cache(CachingStrategy strategy, int size) {
    map = new CacheHashMap(strategy, size);
  }

  public ServerStatus put(String key, String value) {
    map.put(key, value);
    return ServerStatus.SUCCESS;
  }

  public String get(String key) {
    return map.get(key);
  }

  public ServerStatus delete(String key) {
    map.remove(key);
    return ServerStatus.SUCCESS;
  }
  
  public void printCache() {
    System.out.println(map);
  }
}
