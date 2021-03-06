package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.util.Collections;
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
    
    Collections.shuffle(input);
    
    System.out.println("\n" + pref + "DOING GET");
    long getStart = System.nanoTime();
    test(1);
    long getTime = System.nanoTime() - getStart;
    
    Collections.shuffle(input);
    
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
      doLine(line);
    }
  }

  public void doLine(String line) {
    String[] comp = line.split("\\s+", 2);
    //String res;
    try {
      switch (mode) {
        case 0:
           cl.putRequest(comp[0], "\"" + comp[1] + "\"");
          //assertEquals("SUCCESS", result);
          break;
        case 1:
        	cl.getRequest(comp[0]);
          //assertEquals(comp[1], result);
          break;
        case 2:
        	cl.deleteRequest(comp[0]);
          //assertEquals("SUCCESS", result);
          break;
      }
      //System.out.println(pref + res);
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("ioexception");
    }
  }
}
