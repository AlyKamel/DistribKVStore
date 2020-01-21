package de.tum.i13.server.chat;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;

public class Chatroom {
  private final String chatID;
  private final HashMap<String, PrintWriter> users = new HashMap<String, PrintWriter>();
  private ChatBot bot;

  public static enum ChatroomType {PUBLIC, PRIVATE};
  
  public Chatroom(String chatID) {
    this.chatID = chatID;
    try {
      bot = new ChatBot(chatID);
    } catch (IOException e) {
      System.out.println("Error during chatbot creation: " + e.getLocalizedMessage());
    }
  }

  public String addUser(String username, PrintWriter out) {
    if (users.size() < 30) {
      sendToAll(username + " has joined the chatroom");
      users.put(username, out); 
      return "chat_success";
    }
    return "chatroom_full";
  }

  public void removeUser(String username) {
    users.remove(username);
    sendToAll(username + " has left the chatroom");
  }

  public String sendToAll(String msg) {
    String result = bot.msgHandle(msg);
    if (bot.containsPut(msg)) { // return put result
      return result;
    }
    
    if (result.contains(":") || result.contains("chatroom")) { // send message to users
      if (users != null) {
        users.values().forEach(out -> out.println(result));
      }
      return null;
    }
    
    return result; // error message
  }

  public String sendWhisper(List<String> wspList, String msg) {
    for (String u : wspList) {
      if (!users.containsKey(u)) {
        return "User " + u + " was not found in the chatroom";
      }
    }
    
    String finalMsg = bot.replaceGet(msg);
    if (finalMsg.contains(":")) { // send message to users
      wspList.forEach(u -> users.get(u).println(finalMsg)); 
    }
    
    return finalMsg; // error message
  }
  
  public String getUsers() {
    return users.keySet().toString();
  }

  public int userCount() {
    return users.size();
  }

  public ChatroomType getType() {
    return ChatroomType.PUBLIC;
  }

  public String getChatID() {
    return chatID;
  }
}
