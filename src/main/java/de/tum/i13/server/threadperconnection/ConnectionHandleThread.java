package de.tum.i13.server.threadperconnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import de.tum.i13.server.chat.ChatManager;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.threadperconnection.Main.ServerToECS;
import de.tum.i13.shared.Constants;

public class ConnectionHandleThread extends Thread {
  private KVCommandProcessor cp;
  private Socket clientSocket;
  private boolean running = true;
  private ServerToECS ste;

  public ConnectionHandleThread(KVCommandProcessor commandProcessor, Socket clientSocket, ServerToECS ste) {
    this.ste = ste;
    cp = commandProcessor;
    this.clientSocket = clientSocket;
  }

  @Override
  public void run() {
    InetSocketAddress localAddress = (InetSocketAddress) clientSocket.getLocalSocketAddress();
    InetSocketAddress remoteAddress = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
    
    try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING), true);){
      
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Closing thread responsible for client at " + clientSocket.getRemoteSocketAddress());
          running = false;
          cp.connectionClosed(remoteAddress.getAddress());
          out.println("Connection terminated");
        }
      });

      out.println(cp.connected(localAddress, remoteAddress));      
      String username = addUser(in, out);
      ChatManager cs = new ChatManager(username, out, cp);
      
      String line;
      try {
        while (running && (line = in.readLine()) != null) {
          if (!Constants.canEncode(line)) {
            System.out.println("Cannot decode input");
            continue;
          }

          if (line.startsWith("chat")) {
            cs.process(line);
          } else {
            out.println(cp.process(line));
          }
        }
      } catch (SocketException e) {
        if (running) {
          throw new IOException("Client handling thread closed unexpectedly");
        }
      }
      
      ste.removeUser(username);
      cp.connectionClosed(remoteAddress.getAddress());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String addUser(BufferedReader in, PrintWriter out) throws IOException {
    String username = in.readLine();
    String result = ste.addUser(username);
    boolean success = result != null;
    result = "user_" + (success ? "success " + result : "error " + username);
    out.println(result);
    if (!success) {
      return addUser(in, out);
    }
    return username;
  }

  public void updateSR(String newKeyRange) {
    cp.updateServerRing(newKeyRange);
  }

  public void setWriteLock(boolean flag) {
    cp.setWriteLock(flag);
  }
}
