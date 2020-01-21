package de.tum.i13.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Represents a TCP-connection to a server using a Java Socket
 * which allows clients to exchange messages with a server
 */
public class ActiveConnection {

  /**
   * Socket on which the connection is based
   */
  private Socket client;
  
  /**
   * Stream for receiving replies from the server
   */
  private BufferedReader input;
  
  /**
   * Stream for sending messages to the server
   */
  private PrintWriter output;
  
  /**
   * Client will wait at most this long for a response from the server.
   */
  private final int TIMEOUTTIME = 6000;

  /**
   * Creates a new instance and initializes the socket
   */
  public ActiveConnection() {
    reset();
  }

  /**
   * Builds a connection to the specified server
   * @param address Address at which the server is located
   * @param port Port at which the server is located
   * @throws IOException
   */
  public void connect(String address, int port) throws IOException {
    client.connect(new InetSocketAddress(address, port), TIMEOUTTIME);
    //client.setSoTimeout(TIMEOUTTIME);  chat won't work if this is set
    output = new PrintWriter(client.getOutputStream());
    input = new BufferedReader(new InputStreamReader(client.getInputStream()));
  }
  
  /**
   * Closes the socket, the input and the output streams.
   * A new socket is opened in order to allow reconnection.
   * @throws IOException
   */
  public void close() throws IOException { 
    if (connected()) {
      client.close();
      input.close();
      output.close();
    }
    reset();
  }

  /**
   * Restarts the connection by creating a new socket.
   */
  public void reset() {
    client = new Socket();
  }

  /**
   * Sends the specified message to the connected server
   * @param msg Message to be sent
   * @throws IOException
   */
  public void send(String msg) {
    output.println(msg);
    output.flush();
  }

  /**
   * Receives a reply from the connected server
   * @return Message from the server to the client
   * @throws IOException
   */
  public String receive() throws IOException {
    return input.readLine();
  }
  
  public String receiveNoNull() throws IOException {
    String res;
    while ((res = receive()) == null) {}
    return res;
  }
  
  /**
   * Checks if the client is currently connected to a server
   * @return Connection status of the client
   */
  public boolean connected() {
    return client.isConnected();
  }
  
  /**
   * Getter for the address
   * @return Address of the connected server
   */
  public String getAddress() {
    return client.getInetAddress().getHostAddress();
  }
  
  /**
   * Getter for the port
   * @return Port of the connected server
   */
  public int getPort() {
    return client.getPort();
  }
  
  /**
   * Getter for the local socket address
   * @return Local socket address of socket
   */
  public String getLocalSocketAddress() {
    return client.getLocalSocketAddress().toString();
  }
}
