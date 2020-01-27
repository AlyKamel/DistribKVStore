package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.util.List;
import de.tum.i13.client.ClientLibrary;

public class ClientThread extends Thread {
  final String pref;
  final ClientLibrary cl = new ClientLibrary();
  int mode;
  final List<String> input;

  public ClientThread(int clientNum, String addr, int port, List<String> input) {
    this.input = input;
    pref = "client" + clientNum + ": ";
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
    long putStart = System.nanoTime();
    test(0);
    long putTime = System.nanoTime() - putStart;
    
    System.out.println("\n" + pref + "DOING GET");
    long getStart = System.nanoTime();
    test(1);
    long getTime = System.nanoTime() - getStart;
    
    System.out.println("\n" + pref + "DOING DEL");
    long delStart = System.nanoTime();
    test(2);
    long delTime = System.nanoTime() - delStart;
    
    System.out.println(pref + " put: " + (putTime/1.00E+09));
    System.out.println(pref + " get: " + (getTime/1.00E+09));
    System.out.println(pref + " del: " + (delTime/1.00E+09));
  }

  public void test(int mode) {
    this.mode = mode;
    for (String line : input) {
      String result = doLine(line);
      //System.out.println(pref + result);
      
      // try waiting before sending next command
//    try {
//    Thread.sleep(40);
//  } catch (InterruptedException e) {
//    e.printStackTrace();
//  }
    }
  }

  public String doLine(String line) {
    String result = null;
    String[] comp = line.split("\\s+", 2);
    try {
      switch (mode) {
        case 0:
          result = cl.putRequest(comp[0], "\"" + comp[1] + "\"");
          //assertEquals("SUCCESS", result);
          break;
        case 1:
          result = cl.getRequest(comp[0]);
          //assertEquals(comp[1], result);
          break;
        case 2:
          result = cl.deleteRequest(comp[0]);
          //assertEquals("SUCCESS", result);
          break;
        default:
          result = "unknown command, not put, get or del";
      }
    } catch (IOException e) {
      e.printStackTrace();
      result = "ioexception";
    }
    
    return result;
  }
}
