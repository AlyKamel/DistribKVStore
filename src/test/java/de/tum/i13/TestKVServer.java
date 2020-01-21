package de.tum.i13;

import static de.tum.i13.Util.createECSServer;
import static de.tum.i13.Util.createServer;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import de.tum.i13.server.ecs.MainECS;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.ServerUtility;

class TestKVServer {

  private static Thread server;
  private static Thread ecs;
  private static String address;
  private static int serverport;
  private static PrintWriter output;
  private static BufferedReader input;
  private static Socket socket;
  private static String welcome;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    address = "127.0.0.1";
    int ecsport = ServerUtility.getFreePort(address);
    Logger.getLogger(MainECS.class.getName()).setLevel(Level.OFF);
    Logger.getLogger(Main.class.getName()).setLevel(Level.OFF);
    ecs = createECSServer(address, ecsport);
    ecs.start();

    serverport = ServerUtility.getFreePort(address);
    String bootstrap = ServerUtility.getSocketAddress(address, ecsport);
    server = createServer(address, serverport, bootstrap);

    server.start();
    Thread.sleep(1000);
  }
  
  @BeforeEach
  public void beforeEach() throws IOException {
    socket = new Socket(address, serverport);
    output = new PrintWriter(socket.getOutputStream());
    input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    welcome = input.readLine();
    output.println("test_user");
    output.flush();
    input.readLine();
  }

  @AfterEach
  public void afterEach() throws IOException {
    socket.close();
    input.close();
    output.close();
  }
  
  @Test
  public void welcomeTest() throws IOException {
    InetSocketAddress ia = new InetSocketAddress(address, serverport);
    assertEquals("Connection to KV-storage server established: " + ia, welcome);
  }

  @Test
  public void putTest() throws IOException {
    String command = "put key value";
    output.println(command);
    output.flush();

    String res = input.readLine();
    assertThat(res, is(containsString("put_")));
  }

  // tests the get command
  @Test
  public void getTest() throws IOException {
    String command = "get key";
    output.println(command);
    output.flush();

    String res = input.readLine();
    assertThat(res, is(containsString("get_")));
  }

  @Test
  public void deleteTest() throws IOException {
    String command = "delete key";
    output.println(command);
    output.flush();

    String res = input.readLine();
    assertThat(res, is(containsString("delete_")));
  }

  @Test
  public void invalidTest() throws IOException {
    String command = "invalid command";
    output.println(command);
    output.flush();

    String res = input.readLine();
    assertEquals("error: command unrecognized", res);
  }


  // tests whether the server replies in the right order when there is more than one command
  @Test
  public void multipleCommandsTest() throws IOException {
    output.println("put key1 value");
    output.println("get key1");
    output.println("put key2 value");
    output.println("put key3 value");
    output.println("get key4");
    output.println("delete key2");
    output.flush();

    String res = input.readLine();
    assertTrue(res.contains("put_") && res.contains("key1"));
    res = input.readLine();
    assertTrue(res.contains("get_") && res.contains("key1"));
    res = input.readLine();
    assertTrue(res.contains("put_") && res.contains("key2"));
    res = input.readLine();
    assertTrue(res.contains("put_") && res.contains("key3"));
    res = input.readLine();
    assertTrue(res.contains("get_") && res.contains("key4"));
    res = input.readLine();
    assertTrue(res.contains("delete_") && res.contains("key2"));
  }

}
