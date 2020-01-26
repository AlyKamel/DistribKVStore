package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.util.List;
import de.tum.i13.client.ClientLibrary;

public class ClientThread extends Thread {
  private final String pref;
  private final ClientLibrary cl = new ClientLibrary();
  private int mode;
  private final List<String> input;

  public ClientThread(int clientNum, String addr, int port, List<String> input) {
    this.input = input;
    pref = "Client " + clientNum + ": ";
    try {
      cl.connect(addr, port);
      cl.setUsername("client" + clientNum);
    } catch (IOException e) {
      System.out.println(pref + "Connection error");
    }
  }

  @Override
  public void run() {
    System.out.println("\n" + pref + "DOING PUT");
    test(0);
    System.out.println("\n" + pref + "DOING GET");
    test(1);
    System.out.println("\n" + pref + "DOING DEL");
    test(2);
  }

  private void test(int mode) {
    this.mode = mode;
    for (String line : input) {
      String result = doLine(line);
      // System.out.println(pref + result);
      
      // try waiting before sending next command
//    try {
//    Thread.sleep(40);
//  } catch (InterruptedException e) {
//    e.printStackTrace();
//  }
    }
  }

  private String doLine(String line) {
    String result = null;
    String[] comp = line.split("\\s+", 2);
    try {
      switch (mode) {
        case 0:
          result = cl.putRequest(comp[0], "\"" + comp[1] + "\"");
          assertEquals("SUCCESS", result);
          return result;
        case 1:
          result = cl.getRequest(comp[0]);
          assertEquals(comp[1], result);
          return result;
        case 2:
          result = cl.deleteRequest(comp[0]);
          assertEquals("SUCCESS", result);
          return result;
        default:
          return "unknown command, not put, get or del";
      }
    } catch (IOException e) {
      e.printStackTrace();
      return "ioexception";
    }
  }
}
