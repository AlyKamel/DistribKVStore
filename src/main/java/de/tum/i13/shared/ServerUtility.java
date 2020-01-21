package de.tum.i13.shared;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerUtility {
  public static int getFreePort(String address) throws IOException {
    if (address == null) return -1;
    ServerSocket socket = new ServerSocket();
    socket.bind(new InetSocketAddress(address, 0));
    socket.setReuseAddress(true);
    //InetSocketAddress sa = new InetSocketAddress(address, socket.getLocalPort());
    socket.close();
    return socket.getLocalPort();
  }
  
  public static int getFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    socket.setReuseAddress(true);
    socket.close();
    return socket.getLocalPort();
  }
  
  public static String getSocketAddress(String ip, int port) {
    return ip + ":" + port;
  }
  
  public static String getAddress(String socketAddress) {
    return socketAddress.replaceAll(":.*", "");
  }
  
  public static InetSocketAddress getInetSocketAddress(String address) {
    String ip = getAddress(address);
    int port = getPort(address);
    return new InetSocketAddress(ip, port);
  }
  
  public static int getPort(String socketAddress) {
    return Integer.parseInt(socketAddress.replaceAll(".*:", ""));
  }

  /**
   * Keeps trying to connect to the provided address up to 10 tries with a waiting period in between.
   * 
   * @param sa
   * @return socket connected to the specified address
   * @throws IOException if connection has failed
   */
  public static Socket connectNonstop(InetSocketAddress sa) throws IOException {
    int tries = 0;
    Socket s = null;
    while (true) {
      try {
        s = new Socket();
        s.connect(sa, 2000);
        break;
      } catch (ConnectException e) {
        if (++tries > 7) {
          s.close();
          throw new IOException("Unable to connect to " + sa);
        }
        try {
          Thread.sleep(50 * (int) Math.pow(2, tries));
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }  
    return s;
  }
}
