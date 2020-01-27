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
  private static final int serverCount = 1;
  
  private static int[] ports = new int[serverCount];
  private static final String address = "127.0.0.1";
  private static List<String> input;
  
  
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
      Thread server = createServer(address, ports[i], bootstrap);
      server.start();
      Thread.sleep(1500);
    }
  }
  
  @Test
  public void TestSimple() {
    System.out.println("###START###\n");
        
    // setup clients (choose if chatty or not, add chatAmount if chatty)
    ClientThread[] clients = new ClientThread[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new ClientThread(i, address, ports[0], input);
      //clients[i] = new ChattyClientThread(i, address, ports[0], input, chatAmount);
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
