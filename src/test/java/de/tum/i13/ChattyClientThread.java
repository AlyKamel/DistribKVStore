package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.util.List;

public class ChattyClientThread extends ClientThread {

  private int chatAmount;

  public ChattyClientThread(int clientNum, String addr, int port, List<String> input, int chatAmount) {
    super(clientNum, addr, port, input);
    this.chatAmount = chatAmount;
    synchronized (ChattyClientThread.class) {
      try {
        String rep = cl.startChat("testRoom");
        if (rep.equals("chatroom_create")) {
          cl.createPublicChatroom(); 
        }
      } catch (IOException e) {
        System.out.println(pref + "Connection error");
      } 
    }
  }
  
  private void sendMessage() throws IOException {
    for (int i = 0; i < chatAmount; i++) {
      cl.chatSend("o");
    }
    
    for (int i = 0; i < chatAmount; i++) {
      String result = cl.chatReceive(); 
      //System.out.print(result);
    }
  }

  @Override
  public void test(int mode) {
    this.mode = mode;
    for (String line : input) {
      try {
        sendMessage();
      } catch (IOException e) {
        e.printStackTrace();
      }
      doLine(line);
      //System.out.println(pref + result);
    }
  }

  @Override
  public void doLine(String line) {
    String[] comp = line.split("\\s+", 2);
    try {
      switch (mode) {
        case 0:
          cl.chatSend("PUT " + comp[0] + " \"" + comp[1] + "\"");
          cl.chatReceive();
          //assertEquals("SUCCESS", result);
          break;
        case 1:
          cl.chatSend("GET{" + comp[0] + "}");
          cl.chatReceive();
          //assertEquals(pref + comp[1], result);
          break;
        case 2:
          cl.chatSend("PUT " + comp[0]);
          cl.chatReceive();
          //assertEquals("SUCCESS", result);
          break;
        default:
          System.out.println("unknown command, not put, get or del");
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("ioexception");
    }
  }
}
