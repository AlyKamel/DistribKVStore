package de.tum.i13.server.chat;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import de.tum.i13.server.chat.Chatroom.ChatroomType;
import static de.tum.i13.server.chat.Chatroom.ChatroomType.*;
import de.tum.i13.server.kv.KVCommandProcessor;

public class ChatManager {
  private final String username;
  private final PrintWriter pw;
  private Chatroom chatroom;

  private static ArrayList<Chatroom> chs;
  private final KVCommandProcessor kvcp;
  private String chatID;

  public ChatManager(String username, PrintWriter pw, KVCommandProcessor kvcp) {
    this.username = username;
    this.pw = pw;
    synchronized (this) {
      if (chs == null) {
        chs = new ArrayList<Chatroom>();
      }
    }
    this.kvcp = kvcp;
  }

  public void process(String line) {
    String[] tokens = line.split("\\s", 2);
    switch (tokens[0]) {
      case "chatStart": {
        start(tokens[1]);
        break;
      }

      case "chatroomCreate": {
        ChatroomType type = valueOf(tokens[1].split("\\s")[0]);
        String passwordHash = tokens[1].replaceAll(".* ", "");
        createChatroom(type, passwordHash);
        break;
      }

      case "chatVerifyPassword": {
        verifyPassword(tokens[1]);
        break;
      }

      case "chat": {
        send(tokens[1]);
        break;
      }

      case "chatWhisper": {
        String[] t = tokens[1].split("\\s", 2);
        sendWhisper(t[0], t[1]);
        break;
      }

      case "chatActiveUsers": {
        pw.println("Current active users: " + chatroom.getUsers());
        break;
      }

      case "chatEnd": {
        remove();
        break;
      }
    }
  }

  private void start(String chatID) {
    String reply;
    this.chatID = chatID;
    chatroom = chs.stream().filter(c -> c.getChatID().equals(chatID)).findFirst().orElse(null);
    if (chatroom == null) { // chatroom doesn't exist
      reply = chs.size() < 15 ? "chatroom_create" : "chatroom_max";
    } else { // chatroom exists
      reply = chatroom.getType() == PRIVATE ? "chatroom_password_entry" : chatroom.addUser(username, pw);
    }
    pw.println(reply);
  }

  private void createChatroom(ChatroomType type, String passwordHash) {
    chatroom = type == PRIVATE ? new PrivateChatroom(chatID, passwordHash) : new Chatroom(chatID);
    chatroom.addUser(username, pw);
    chs.add(chatroom);
  }

  private void verifyPassword(String passwordHash) {
    synchronized (ChatManager.class) {
      ((PrivateChatroom) chatroom).verifyPassword(passwordHash);
      pw.println(chatroom.addUser(username, pw));
    }
  }

  private void sendWhisper(String users, String msg) {
    List<String> wspList = Arrays.asList(users.split(","));
    if (wspList.contains(username)) {
      pw.println("You can't whisper to yourself!");
      return;
    }

    msg = "Whisper from " + prependUsername(msg);
    pw.println(chatroom.sendWhisper(wspList, msg));
  }

  private void send(String msg) {
    msg = prependUsername(msg);
    String result = chatroom.sendToAll(msg);
    if (result == null) {
      kvcp.kvs.chatAppend(chatroom.getChatID(), msg);
    } else {
      pw.println(result);
    }
  }

  private void remove() {
    chatroom.removeUser(username);
    if (chatroom.userCount() == 0) {
      chs.remove(chatroom);
    }
    chatroom = null;
    pw.println("QUIT");
  }

  private String prependUsername(String msg) {
    return username + ": " + msg;
  }
}
