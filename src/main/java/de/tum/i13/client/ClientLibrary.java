package de.tum.i13.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.shared.ServerUtility;

/**
 * The ClientProgram implements an application logic, a simple command line-based user interface
 * that enables user to communicate with the server typing the given commands. The program prints
 * the output on the screen based on the commands.
 */
public class ClientLibrary {
  public static Logger logger = Logger.getLogger(ClientApp.class.getName());
  private ActiveConnection ac = new ActiveConnection();
  private HashRing hr = new HashRing();
  private String serverAddress;
  private ExpBackoff expbackoff = new ExpBackoff();
  private String username;

  // CLIENT KV-INTERFACE
  /**
   * Connects to server using the ServerConnection object of the class. The parameters contain the
   * specific address and port of the server it is trying to connect to. Will write on console and
   * log.
   * 
   * @param address address of the remote server
   * @param port port of the remote server
   * @throws IOException
   * 
   */
  public String connect(String address, int port) throws IOException {
    ac.connect(address, port);
    String reply = ac.receive();
    logger.info(reply.replace("\r\n", ""));
    serverAddress = ServerUtility.getSocketAddress(address, port);
    if (username != null) {
      sendUsername(); 
    }
    return reply;
  }
  
  private String sendUsername() throws IOException {
    ac.send(username);
    String reply = ac.receiveNoNull();
    if (reply.startsWith("user_success")) {
      username = reply.substring(13);
      return username;
    }
    return null;
  }

  public String setUsername(String username) throws IOException {
    this.username = username;
    return sendUsername();
  }
  
  public String getUsername() {
    return username;
  }
  
  public boolean connected() {
    return ac.connected();
  }
  
  /**
   * If there is currently a connection via the ServerConnection object of this class, then this
   * connection (socket) will be closed. Will write on console and log.
   * 
   * @throws IOException
   */
  public String disconnect() throws IOException {
    if (!connected()) {
      return null;
    }
    close();
    String closingmessage = "Connection terminated: /" + serverAddress;
    serverAddress = null;
    return closingmessage;
  }

  /**
   * Closes the connection via socket to the currently connected server. Will write on console and
   * log.
   */
  public void close() throws IOException {
    ac.close();
  }

  /**
   * Sends a get request to the server to retrieve the value stored in the KVStore, if it exists.
   * Will write on console and log.
   * 
   * @param key key of the value sought-after
   * @return a message to the client with consideration of the servers reply
   */
  public String getRequest(String key) throws IOException {
    logger.finer("Getting value associated with key \"" + key + "\"..");
    connectToReadResponsibleServer(key);
    ac.send("get " + key);
    String reply = ac.receive();
    logger.finest(reply);
    String result = checkReadResponse(reply, key);
    if (result.equals("retry")) {
      return getRequest(key);
    }
    if (result.startsWith("No value")) {
      return result;
    }
    return result.split("\\s", 3)[2];
  }
  
  /**
   * Parses the input for the key and value and sends a put request to the server. A put request
   * either inserts a new key-value pair into the KVStore, updates an existing value or deletes the
   * key. Will write on console and log.
   * 
   * @param key
   * @param value
   * @return a message to the client with consideration of the servers reply
   * @throws IOException
   * 
   */
  public String putRequest(String key, String value) throws IOException {
    logger.finer("Setting the value \"" + value + " to key \"" + key + "\"..");
    if (value.equals("\"No value found for given key\"")) {
      return "ERROR: This value is reserved."; // used when no value exists for the key
    }
    
    connectToCoordinator(key);
    ac.send("put " + key + " " + value);
    String reply = ac.receive();
    logger.finest(reply);
    if (reply.contains("put")) {
      String status = reply.split("\\s|_")[1];
      return status.toUpperCase();
    } else if (checkWriteBlock(reply, key)) {
      return "Storage server is currently blocked for write requests due to reallocation";
    }
    return putRequest(key, value);
  }

  /**
   * Sends a delete request to the server. A delete request deletes the value of the given key. Will
   * write on console and log.
   * 
   * @param key
   * @return a message to the client with consideration of the servers reply
   * @throws IOException
   * 
   */
  public String deleteRequest(String key) throws IOException {
    logger.finer("Removing key \"" + key + "\" from storage..");
    connectToCoordinator(key);
    ac.send("delete " + key);
    String reply = ac.receive();
    logger.finest(reply);
    if (reply.contains("delete")) {
      String status = reply.split("\\s|_")[1];
      return status.toUpperCase();
    } else if (checkWriteBlock(reply, key)) {
      return "Storage server is currently blocked for write requests due to reallocation";
    }
    return deleteRequest(key);
  }

  
  // HELPER METHODS
  /**
   * checks for server replies in case of not_responsible, server_stopped or server_write_lock
   * when get request is called
   * 
   * @param reply
   * @param key
   * @throws IOException
   */
  private String checkReadResponse(String reply, String key) throws IOException {
    String status = reply.split("\\s|_")[1];
    switch (status) {
      case "success":
      case "update": {
        return reply;
      }
      case "error": {
        return "No value found for given key";
      }
      case "not": {
        updateReadKeyRange();
        connectToReadResponsibleServer(key);
        return "retry";
      }
      case "stopped": {
        serverStopped();
        return "retry";
      }
      default: {
        logger.info("Unexpected server response");
        disconnect();
        throw new IOException();
      }
    }
  }

  /**
   * checks for server replies in case of not_responsible, server_stopped or server_write_lock
   * when put or delete requests are called
   * 
   * @param reply
   * @param key
   * @throws IOException
   */
  private boolean checkWriteBlock(String reply, String key) throws IOException {
    String status = reply.split("\\s|_")[1];
    switch (status) {
      case "write": {
        return true;
      }
      case "not": {
        updateKeyRange();
        connectToCoordinator(key);
        return false;
      }
      case "stopped": {
        serverStopped();
        return false;
      }
      default: {
        logger.info("Unexpected server response");
        disconnect();
        throw new IOException();
      }
    }
  }

  /**
   * server is at state stopped and ExpBackoff class is called for the use of 
   * exponential back-off with jitter 
   */
  private void serverStopped() {
    logger.fine("server " + serverAddress + " is currently stopped");
    try {
      expbackoff.waitExp(serverAddress);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * in case of the key does not belong to the keyrange of the current server for 
   * put and delete requests it disconnects from current server and connects to the coordinator server
   * 
   * @param key
   * @throws IOException
   */
  private void connectToCoordinator(String key) throws IOException {
    logger.finer("Connecting to the coordinator");
    String server = hr.getCoordinator(key);
    if (server == null) {
      updateKeyRange();
      server = hr.getCoordinator(key);
    }
    if (serverAddress.equals(server)) {
      return;
    }

    reconnect(server);
  }

  /**
   * in case of the key does not belong to the keyrange of the current server for
   * get requests it disconnects from current server and connects to the coordinator server or
   * one of the replicas randomly
   * 
   * @param key
   * @throws IOException
   */
  private void connectToReadResponsibleServer(String key) throws IOException {
    logger.finer("Connecting to a responsible read server");
    ArrayList<String> servers = hr.getReadResponsibleServers(key);
    if (servers.get(0) == null) {
      updateReadKeyRange();
      servers = hr.getReadResponsibleServers(key);
    }
    if (servers.contains(serverAddress)) {
      return;
    }

    int serverIndex = hr.replicationActive() ? new Random().nextInt(3) : 0;
    String server = servers.get(serverIndex);
    reconnect(server);
  }
  
  private void reconnect(String server) throws IOException {
    disconnect();
    String ip = ServerUtility.getAddress(server);
    int port = ServerUtility.getPort(server);
    connect(ip, port);
  }

  /**
   * sends a keyrange request to the server and updates the keyrange and sets the keyrange for the
   * hashring
   * 
   * @throws IOException
   */
  private void updateKeyRange() throws IOException {
    String reply;
    do {
      logger.finer("Updating key range");
      ac.send("keyrange");
      reply = ac.receive();
    } while (!checkUpdateResponse(reply));

    String keyrange = reply.substring(17); // removes header
    hr.setKeyRange(keyrange);
  }

  /**
   * sends a keyrange request to the server and updates the keyrange for get requests and sets it
   * for the hashring
   * 
   * @throws IOException
   */
  private void updateReadKeyRange() throws IOException {
    String reply;
    do {
      logger.finer("Updating read key range");
      ac.send("keyrange_read");
      reply = ac.receive();
    } while (!checkUpdateResponse(reply));

    String keyrange = reply.substring(22); // removes header
    hr.setReadKeyRange(keyrange);
  }

  /**
   * checks the update response from the server
   * 
   * @param reply
   * @throws IOException
   */
  private boolean checkUpdateResponse(String reply) throws IOException {
    if (reply.startsWith("keyrange")) {
      return true;
    } else if (reply.equals("server_stopped")) {
      serverStopped();
      return false;
    }
    logger.info("Unexpected server response");
    disconnect();
    throw new IOException();
  }
  
  
  // CHAT COMMANDS
  public String startChat(String chatID) throws IOException {
    connectToCoordinator(chatID);
    ac.send("chatStart " + chatID);
    String reply = ac.receiveNoNull();
    switch (reply) {
      case "chatroom_password_invalid": {
        return "Incorrect password.";
      }
      
      case "chatroom_full":{
        return "Maximum amount of users in this chatroom reached.";
      }
      
      case "chatroom_max": {
        return "Maximum amount of chatrooms on this server reached.";
      }
      
      default: return reply;
    }
  }
  
  public void createPublicChatroom(){
    ac.send("chatroomCreate PUBLIC");
  }

  public void createPrivateChatroom(String password) {
    password = hr.hash(password);
    ac.send("chatroomCreate PRIVATE " + password);
  }
  
  public String sendChatroomPWD(String password) throws IOException {
    password = hr.hash(password);
    ac.send("chatVerifyPassword " + password);
    return ac.receiveNoNull();
  }
  
  public void chatSend(String msg) throws IOException {
    ac.send("chat " + msg); 
  }
  
  public String chatReceive() throws IOException {
    return ac.receiveNoNull();
  }

  public void chatWhisper(String user, String msg) {
    ac.send("chatWhisper " + user + " " + msg);
  }
  
  public void getActiveChatUsers() {
    ac.send("chatActiveUsers");
  }
  
  public void endChat() throws IOException {
    ac.send("chatEnd");
  }
}
