package de.tum.i13;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.server.kv.DiskStore;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.caching.Cache;
import de.tum.i13.server.threadperconnection.ServerRing;

public class TestKVCommandProcessor {

  private static KVCommandProcessor cp;
  private static Cache ch = mock(Cache.class);
  private static DiskStore disk = mock(DiskStore.class);
  
  @BeforeAll
  public static void beforeAll() {
    KVStore kvs = new KVStore(disk, ch);
    HashRing hr = new HashRing();
    hr.addServer("testserver");
    ServerRing sr = new ServerRing(hr, "testserver");
    cp = new KVCommandProcessor(kvs, sr);
  }
  
  @Test
  public void correctParsingOfPut() throws Exception {      
    cp.process("put key hello");
    verify(ch).put("key", "hello");
    verify(disk).put("key", "hello");
  }

  @Test
  public void correctParsingOfGet() throws Exception { 
    cp.process("get key");
    verify(ch).get("key");
    verify(disk).get("key");
  }

  @Test
  public void correctParsingOfDelete() throws Exception {
    cp.process("delete key");
    verify(ch).delete("key");
    verify(disk).delete("key");
  }
}
