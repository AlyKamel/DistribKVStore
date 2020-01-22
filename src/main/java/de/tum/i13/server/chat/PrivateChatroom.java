package de.tum.i13.server.chat;

import java.io.PrintWriter;

public class PrivateChatroom extends Chatroom {
  private String passwordHash;
  private boolean verified = true;
  
  public PrivateChatroom(String chatID, String passwordHash, String kvAddress) {
    super(chatID, kvAddress);
    this.passwordHash = passwordHash;
  }
  
  public void verifyPassword(String passwordHash) {
    verified = this.passwordHash.equals(passwordHash);
  }
  
  @Override
  public String addUser(String username, PrintWriter out) {
    if (verified) {
      verified = false;
      return super.addUser(username, out);
    }
    return "chatroom_password_invalid";
  }
  
  @Override
  public ChatroomType getType() {
    return ChatroomType.PRIVATE;
  }
}
