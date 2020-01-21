package de.tum.i13.server.chat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import de.tum.i13.client.ClientLibrary;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.ServerUtility;

/*
 * This object is designed to help the chatrooms to access the distributed database. It shares a lot
 * of similarities with the ClientApp.java class.
 */
public class ChatBot {
  /*
   * Defined maximum size of the key length
   */
  private static final int MAXKEYSIZE = 20;
  /*
   * Defined maximum size of the value length
   */
  private static final int MAXVALUESIZE = 122880;

  /*
   * Used for socket communication with the distributed database.
   */
  private final ClientLibrary cl = new ClientLibrary();

  /*
   * Name of the BOT, regarding the chatroom
   **/
  private final String botID;

  /**
   * Constructor of the class. The BOT should always connect first to its origin server.
   * 
   * @param chatID ID of the BOT, used for naming
   * @param address Address of the server he tries to connect to
   * @param port Port of the server he tries to connect to
   */
  public ChatBot(String chatID) throws IOException {
    botID = "BOT_" + chatID;
    String address = ServerUtility.getAddress(Main.kvAddress);
    int port = ServerUtility.getPort(Main.kvAddress);
    cl.connect(address, port);
    cl.setUsername(botID);
  }
    
 /**
  * Takes the message and checks for put, delete or get commands. If
  * found it calls the BOT object to execute these commands and 
  * overwrites the input former message.
  * 
  * @param msg Message to be potentially modified
  * @return Input message with filled gaps or a success/fail notification
  * */
 public String msgHandle(String msg) {
   int userIndex = msg.indexOf(":") + 2;
   String msgNoUser = msg.substring(userIndex);
   
   if (containsPut(msgNoUser)) {
     Matcher mr = Pattern.compile("^PUT (\\S+) (.+)").matcher(msgNoUser);
     if (mr.matches()) {
       String key = mr.group(1);
       String value = mr.group(2);
       return put(key, value);
     }
     
     mr = Pattern.compile("^PUT (\\S+)").matcher(msgNoUser);
     if (mr.matches()) {
       String key = mr.group(1);
       return delete(key);
     }
   }

   return replaceGet(msg);
 }
 
 public boolean containsPut(String msg) {
   return msg.matches("^PUT .+");
 }
 
 /**
  * Searches for any structures in the form GET{key} and replaces them
  * with the information from the database (if value is found).
  * 
  * @param msg Message to be potentially modified
  * @return Input message with filled gaps
  * */
 public String replaceGet(String msg) {
   Matcher mr = Pattern.compile("(?<!\\\\)GET\\{(\\S+)\\}").matcher(msg);
   if (!mr.find()) {
     return msg;
   }
   
   mr.reset();
   StringBuffer sb = new StringBuffer();
   while (mr.find()) {
     String key = mr.group(1);
     if (key.length() > MAXKEYSIZE) {
       return "Maxmimum key length exceeded";
     }
     
     String value;
     try {
       value = cl.getRequest(key);
     } catch (IOException e) {
       return "Error during retrieval process";
     }
     
     if (value.equals("No value found for given key")) {
       return value;
     }
     mr.appendReplacement(sb, value);
   }
   mr.appendTail(sb);
   return sb.toString();
 }  

  /**
   * The BOT executes a put command on the database and on success returns a corresponding answer.
   * Otherwise there will be some type of error message.
   * 
   * @param key key of the KV-pair for the put command
   * @param value value of the KV-pair for the put command
   * @return on success the corresponding value, otherwise an error message
   */
  private String put(String key, String value) {
    if (key.length() > MAXKEYSIZE) {
      return "Maxmimum key length exceeded";
    } else if (value.length() > MAXVALUESIZE) {
      return "Maxmimum value length exceeded";
    } else if (value.contains(" ") && !(value.startsWith("\"") && value.endsWith("\""))) {
      return "Invalid input! Please surround your value with quotation marks (e.g. PUT key \"my very long value\")";
    }

    try {
      return cl.putRequest(key, value);
    } catch (IOException e) {
      return "Error during storing process";
    }
  }

  /**
   * The BOT executes a delete command on the database and on success returns a corresponding
   * answer. Otherwise there will be some type of error message.
   * 
   * @param key key of the KV-pair for the delete command
   * @return success or error message
   */
  private String delete(String key) {
    if (key.length() > MAXKEYSIZE) {
      return "Maxmimum key length exceeded";
    }
    try {
      return cl.deleteRequest(key);
    } catch (IOException e) {
      return "Error during deletion process";
    }
  }
}
