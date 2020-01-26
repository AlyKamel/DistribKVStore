package de.tum.i13;

import static de.tum.i13.Util.createECSServer;
import static de.tum.i13.Util.createServer;
import static de.tum.i13.shared.ServerUtility.getFreePort;
import static de.tum.i13.shared.ServerUtility.getSocketAddress;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class Testsimple {

  private static final int clientCount = 1;
  private static final int serverCount = 4;
  private static int[] ports = new int[serverCount];
  private static final String address = "127.0.0.1";
  private static final Thread[] servers = new Thread[serverCount];
  private static List<String> input;

  // currently works for:
  // 1S1C, 2S1C, 3S1C
 
  
  @BeforeAll
  public static void setup() throws IOException, InterruptedException {
    // set input
    Path path = Path.of("bi320.txt");
    input = Files.readAllLines(path);
    
    // start ecs
    int ecsport = getFreePort(address);
    String bootstrap = getSocketAddress(address, ecsport);
    Thread ecs = createECSServer(address, ecsport);
    ecs.start();

    // start servers
    for (int i = 0; i < serverCount; i++) {
      ports[i] = getFreePort(address);
      servers[i] = createServer(address, ports[i], bootstrap);
      servers[i].start();
      Thread.sleep(2000);
    }

    Thread.sleep(2000);
  }

  @Test
  public void TestSimple() {
    System.out.println("###START###\n");
    
    // setup clients
    ClientThread[] clients = new ClientThread[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new ClientThread(i, address, ports[0], input);
    }

    // start clients
    for (int i = 0; i < clientCount; i++) {
      clients[i].start();
      System.out.println("Client" + i + ": Start");
    }

    // wait for clients
    for (int i = 0; i < clientCount; i++) {
      try {
        clients[i].join();
      } catch (InterruptedException e) {
        System.out.println("Client " + i + ": Fail");
      }
      System.out.println("Client" + i + ": End");
    }
    System.out.println("\n###END###\n");
  }
}
