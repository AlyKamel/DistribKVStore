package de.tum.i13.server.kv;

public interface KVStorageSystem {
  public ServerStatus put(String key, String value);

  public String get(String key);

  public ServerStatus delete(String key);
}
