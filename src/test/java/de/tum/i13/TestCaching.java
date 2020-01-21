package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.server.kv.DiskStore;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.caching.Cache;
import de.tum.i13.server.kv.caching.CachingStrategy;
import de.tum.i13.server.threadperconnection.ServerRing;

class TestCaching {

  private static DiskStore disk = mock(DiskStore.class);
  private static ServerRing sr;
  
  @BeforeAll
  public static void beforeAll() {
    HashRing hr = new HashRing();
    hr.addServer("testserver");
    sr = new ServerRing(hr, "testserver");
  }
  
  @Test
  public void FIFOTest() throws IOException {
    Cache ch = new Cache(CachingStrategy.FIFO, 5);
    KVStore kvs = new KVStore(disk, ch);
    KVCommandProcessor kvcp = new KVCommandProcessor(kvs, sr);
    
    for (int i = 1; i <= 5; i++) {
      kvcp.process("put key" + i + " val" + i);
    }

    kvcp.process("get key1");
    kvcp.process("put key6 val6");

    // key1 should be deleted
    assertNull(ch.get("key1"));

    for (int i = 2; i <= 6; i++) {
      assertEquals("val" + i, ch.get("key" + i));
    }
  }

  @Test
  public void LRUTest() throws IOException {
    Cache ch = new Cache(CachingStrategy.LRU, 5);
    KVStore kvs = new KVStore(disk, ch);
    KVCommandProcessor kvcp = new KVCommandProcessor(kvs, sr);
    
    for (int i = 1; i <= 5; i++) {
      kvcp.process("put key" + i + " val" + i);
    }

    kvcp.process("get key1");
    kvcp.process("put key2 val2");
    kvcp.process("put key6 val6");

    // key3 should be deleted
    for (int i = 1; i <= 6; i++) {
      if (i == 3) {
        assertNull(ch.get("key" + i));
        break;
      }
      assertEquals("val" + i, ch.get("key" + i));
    }
  }

  @Test
  public void LFUTest() throws IOException {
    Cache ch = new Cache(CachingStrategy.LFU, 5);
    KVStore kvs = new KVStore(disk, ch);
    KVCommandProcessor kvcp = new KVCommandProcessor(kvs, sr);

    for (int i = 1; i <= 5; i++) {
      kvcp.process("put key" + i + " val" + i);
    }

    kvcp.process("get key3");
    kvcp.process("get key1");
    kvcp.process("get key1");
    kvcp.process("get key2");

    kvcp.process("put key4 val4");
    kvcp.process("put key6 val6");

    // key5 should be deleted
    for (int i = 1; i <= 5; i++) {
      if (i == 5) {
        assertNull(ch.get("key" + i));
        break;
      }
      assertEquals("val" + i, ch.get("key" + i));
    }
  }

}

