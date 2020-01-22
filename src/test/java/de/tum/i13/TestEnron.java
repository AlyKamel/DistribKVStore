package de.tum.i13;

import static de.tum.i13.Util.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import de.tum.i13.client.ClientLibrary;
import de.tum.i13.server.ecs.MainECS;
import de.tum.i13.server.threadperconnection.Main;
import static de.tum.i13.shared.ServerUtility.*;
import static de.tum.i13.shared.ServerUtility.getFreePort;

class TestEnron {
  
  private static final int serverCount = 5;
  private static int[] ports = new int[serverCount];
  private static final String address = "127.0.0.1";
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
      servers[i] = createServer(address, ports[i], bootstrap);
      servers[i].start();
      Thread.sleep(3000);
    }
    
    Thread.sleep(2000);  
  } 
  
  @Test
  public void TestEnronSet() throws IOException, InterruptedException {    
    ClientLibrary cl = new ClientLibrary();
    cl.connect(address, ports[0]);
    cl.setUsername("test_user");
    
    BufferedReader enronSet = new BufferedReader(new FileReader("bi320.txt"));  
    String line;
    while ((line = enronSet.readLine()) != null) {
      String[] comp = line.split("\\s+", 2);
      cl.putRequest(comp[0], "\"" + comp[1]+ "\""); // adding quotation marks
    }
    enronSet.close();
    
    enronSet = new BufferedReader(new FileReader("bi320.txt"));  
    while ((line = enronSet.readLine()) != null) {
      String[] comp = line.split("\\s+", 2);
      assertEquals(comp[1], cl.getRequest(comp[0]));
    }
    enronSet.close(); 

    cl.disconnect();
  }
}
