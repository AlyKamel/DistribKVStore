package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import de.tum.i13.server.ecs.HashRing;

public class TestHashing {

  @Test
  public void singleServer() {
    HashRing hr = new HashRing();
    hr.addServer("server1");  
    assertEquals("a8438da78e679f44a5cff9e44ebacfbd", hr.hash("server1"));
    
    assertTrue(hr.isCoordinator("server1", "server1"));
    assertTrue(hr.isCoordinator("server1", "test1"));
    assertTrue(hr.isCoordinator("server1", "test2"));
    assertTrue(hr.isCoordinator("server1", "test3"));
    assertTrue(hr.isCoordinator("server1", "z"));
  }
  
  @Test
  public void twoServers() {
    HashRing hr = new HashRing();
    hr.addServer("server1");  
    hr.addServer("server2");
    assertEquals("194f9987498c1cf5a795d83caa147814", hr.hash("server2"));
    
    assertFalse(hr.isCoordinator("server1", "server2"));
    assertTrue(hr.isCoordinator("server2", "server2"));
    assertFalse(hr.isCoordinator("server2", "test1"));
    assertFalse(hr.isCoordinator("server1", "z"));
  }
  
  @Test
  public void singleServerAfterDelete() throws IOException {
    HashRing hr = new HashRing();
    hr.addServer("server1");  
    hr.addServer("server2");
    hr.removeServer("server1");
    
    assertTrue(hr.isCoordinator("server2", "server2"));
    assertTrue(hr.isCoordinator("server2", "server1"));
    assertTrue(hr.isCoordinator("server2", "test1"));
    assertTrue(hr.isCoordinator("server2", "z"));
  }
}
