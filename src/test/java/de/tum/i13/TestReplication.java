package de.tum.i13;

import static de.tum.i13.Util.createECSServer;
import static de.tum.i13.Util.createServer;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import de.tum.i13.client.ActiveConnection;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.server.ecs.MainECS;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.ServerUtility;
import static de.tum.i13.shared.ServerUtility.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestReplication {
  private static final int serverCount = 3;
  private static final HashRing hr = new HashRing();
  private static final String[] addressArray = new String[serverCount];
  private static int[] ports = new int[serverCount];
  private static final String address = "127.0.0.1";
  private static final ActiveConnection ac = new ActiveConnection();
  private static final Thread[] servers = new Thread[serverCount];
  
  @BeforeAll
  public static void setup() throws IOException, InterruptedException{
    Logger.getLogger(MainECS.class.getName()).setLevel(Level.ALL);
    Logger.getLogger(Main.class.getName()).setLevel(Level.ALL);

    int ecsport = getFreePort(address);
    String bootstrap = getSocketAddress(address, ecsport);
    Thread ecs = createECSServer(address, ecsport);
    ecs.start();
    
    for (int i = 0; i < serverCount; i++) {
      ports[i] = getFreePort(address);
      addressArray[i] = address + ":" + ports[i];
      hr.addServer(addressArray[i]);
      servers[i] = createServer(address, ports[i], bootstrap);
      servers[i].start();
      Thread.sleep(600);
    }
    
    Thread.sleep(600);
  }
  
  private void setupConnection(int port) throws IOException {
    ac.connect(address, port);
    ac.receive();
    ac.send("test_user" + port);
    ac.receive();
  }

  // fails/does not terminate sometimes for 2+ servers
  /**
   * This test stores a value in a server then checks that it can be retrieved only from that server and both of its replicas.
   * @throws IOException
   */
  @Test
  public void testCorrectReplication() throws IOException {
    String testKey = "testKey";
    String testValue = "X";
    String success = "get_success " + testKey + " " + testValue;
    String error = "server_not_responsible";

    String coordinator = hr.getCoordinator(testKey);
    int coordinatorPort = ServerUtility.getPort(coordinator);
    //System.out.println("key: " + testKey);
    
    setupConnection(coordinatorPort);
    ac.send("put " + testKey + " " + testValue);
    System.out.println(ac.receive());
    ac.close();

    for (int i = 0; i < addressArray.length; i++) {
      setupConnection(ports[i]);
      ac.send("get " + testKey);
      String reply = ac.receive();

      System.out.println("TEST: " + addressArray[i] + ": " + reply);
      if (hr.isReadResponsible(addressArray[i], testKey)) {
        assertEquals(success, reply);
      } else {
        assertEquals(error, reply);
      }
      ac.close();
    }   
  }
}
