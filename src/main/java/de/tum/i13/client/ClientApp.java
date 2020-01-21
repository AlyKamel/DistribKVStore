package de.tum.i13.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import de.tum.i13.server.chat.Chatroom.ChatroomType;
import de.tum.i13.shared.LogSetup;
import de.tum.i13.shared.LogeLevelChange;
import static de.tum.i13.shared.Constants.canEncode;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * The ClientApp implements an application logic, a simple command line-based user interface that
 * enables user to communicate with the server typing the given commands. The program prints the
 * output on the screen based on the commands.
 */
public class ClientApp {
  public static Logger logger = Logger.getLogger(ClientApp.class.getName());

  private final int MAXKEYSIZE = 20;
  private final int MAXVALUESIZE = 122880;
  private final ClientLibrary cl = new ClientLibrary();
  private final BufferedReader cons = new BufferedReader(new InputStreamReader(System.in)); 

  public static void main(String args[]) {
    Path logfile = Paths.get("client.log");
    setupLogging(logfile, "INFO");
    ClientApp app = new ClientApp();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Exiting application...");
      app.close();
    }));

    app.start();
  }

  private void start() {
    logger.info("Starting application...");

    while (true) {
      System.out.print("Client> ");

      String line = read();
      String[] tokens = line.split("\\s+", 3);
      String opt = tokens[0];
      int argNum = tokens.length - 1;

      logger.finest("Input: " + String.join(" ", tokens));

      if (opt.equals("connect") && argNum == 2) {
        connect(tokens[1], tokens[2]);
      } else if (opt.equals("disconnect") && argNum == 0) {
        disconnect();
      } else if (opt.equals("put") && argNum >= 1 && argNum <= 2) {
        putRequest(tokens);
      } else if (opt.equals("get") && argNum == 1) {
        getRequest(tokens[1]);
      } else if (opt.equals("chat") && argNum == 1) {
        chat(tokens[1]);
      } else if (opt.equals("logLevel") && argNum == 1) {
        changeLogLevel(tokens);
      } else if (opt.equals("help") && argNum == 0) {
        printHelp();
      } else if (opt.equals("quit") && argNum == 0) {
        close();
        printLine("Application exit!");
        break;
      } else if (!opt.isEmpty()) {
        printLine("Unknown command");
        printHelp();
      }
    }
  }

  /**
   * Reads input from the console and returns it in a string. Will write on console and log.
   * 
   * @return input as string array
   */
  private String read() {
    String input = "";
    try {
      while (input.isBlank()) {      
        input = cons.readLine().trim();
      }
      if (!canEncode(input)) {
        printLine("Input contains a character which is not in ISO-8859-1 character set");
        return "";
      }
    } catch (IOException e) {
      printLine("Fatal error: input cannot be read. Quitting application...");
      logger.severe("Input error: " + e.getMessage());
      System.exit(1);
    }
    return input;
  }

  /**
   * Connects to server using the ServerConnection object of the class. The parameters contain the
   * specific address and port of the server it is trying to connect to. Will write on console and
   * log.
   * 
   * @param address address of the remote server
   * @param port port of the remote server
   * 
   */
  private void connect(String address, String port) {
    if (cl.connected()) {
      printLine("Connection already established");
      return;
    }

    try {
      printLine(cl.connect(address, Integer.parseInt(port)));
      setUsername();
      printLine("Welcome " + cl.getUsername() + "!");
    } catch (NumberFormatException e) {
      printLine("Invalid port");
    } catch (IOException e) {
      printLine("Error: unable to connect to server!");
      logger.warning("Connection error: " + e.getMessage());
      close();
    }
  }

  private void setUsername() throws IOException {
    String username;
    while (true) {
      while (true) {
        printLine("Enter your username: (type QUIT to cancel)");
        username = read().trim();
        if (username.contains(",") || username.contains("{") || username.contains("}") || username.contains(":")) {
          printLine("Commas, curly brackets and colons are not allowed in username.");
        } else if (username.startsWith("BOT")) {
          printLine("Name reserved for chatroom bots");
        } else if (checkInput(username, "username", 15)) {
          break;
        }
      }

      if ((username = cl.setUsername(username)) != null) {
        break;
      }
      
      printLine("Username taken.");
    }
  }
 
  /**
   * If there is currently a connection via the ServerConnection object of this class, then this
   * connection (socket) will be closed. Will write on console and log.
   */
  private void disconnect() {
    if (!cl.connected()) {
      printLine("Application not connected");
      return;
    }

    try {
      printLine(cl.disconnect());
    } catch (IOException e) {
      printLine("Error during disconnecting");
    }
  }

  /**
   * Parses the input for the key and value and sends a put request to the server. A put request
   * either inserts a new key-value pair into the KVStore, updates an existing value or deletes the
   * key. Will write on console and log.
   * 
   * @param tokens client input
   */
  private void putRequest(String[] tokens) {
    if (!cl.connected()) {
      printLine("Error! Not connected!");
      return;
    }

    String key = tokens[1];
    if (!checkInput(key, "key", MAXKEYSIZE)) {    
    } else if (tokens.length == 2) { // delete
      try {
        printLine(cl.deleteRequest(key));
      } catch (IOException e) {
        printLine("Error during deletion process");
      }
    } else { // store/update
      String value = tokens[2];
      if (value.length() > MAXVALUESIZE) {
        printLine("Invalid input! Value is too long!");
      } else if (value.contains(" ") && !(value.startsWith("\"") && value.endsWith("\""))) {
        printLine("Invalid input! Please surround your value with quotation marks (e.g. put key \"my very long value\")");
      } else {
        try {
          printLine(cl.putRequest(key, value));
        } catch (IOException e) {
          printLine("Error during storing process");
        }
      }
    }
  }

  /**
   * Sends a get request to the server to retrieve the value stored in the KVStore, if it exists.
   * Will write on console and log.
   * 
   * @param key key of the value sought-after
   */
  private void getRequest(String key) {
    if (!cl.connected()) {
      printLine("Error! Not connected!");
    } else if (!checkInput(key, "key", MAXKEYSIZE)) {    
    } else {
      try {
        printLine(cl.getRequest(key));
      } catch (IOException e) {
        printLine("Error during retrieval process");
      }
    }
  }

  /**
   * Starts a chat session at the chatroom with the provided chatID. The user is able to send messages to and receive messages from all users in this chatroom.
   * Will write on log and console.
   * @param chatID
   */
  private void chat(String chatID) {
    if (!cl.connected()) {
      printLine("Error! Not connected!");
      return;
    }

    // thread responsible to send chat messages
    Thread senderThread = new Thread() {  
      private final String helpMessage = "Following commands are possible during chat:\n"
          + "PUT <key> <value>            --> Stores the given value and allows future access to it through the provided key.\n"
          + "PUT <key>                    --> Deletes the value assigned to the given key.\n"
          + "GET{<key>}                   --> Inserts the value assigned to the given key into the message.\n"
          + "WSP <user1>,..,<userN> <msg> --> Sends a whisper to the users provided\n"
          + "QUIT                         --> Leaves this chat session\n"
          + "ACTIVE                       --> Returns a list of all users in this chatroom\n"
          + "HELP                         --> Displays this text";
      
      private boolean checkMessageSize(String msg) {
        if (msg.length() > 200) {
          printLine("Maximum message size exceeded");
        }
        return msg.length() <= 200;
      }
      
      @Override
      public void run() {
        try {
          boolean running = true;
          while (running) {
            String msg = read();
            
            if (msg.matches("WSP \\S+ .+")) {
              String[] tokens = msg.split("\\s", 3);
              if (checkMessageSize(tokens[2])) {
                cl.chatWhisper(tokens[1], tokens[2]);
              }
              continue;
            }
            
            switch (msg) {
              case "QUIT": {
                running = false;
                break;
              }
              case "HELP": {
                System.out.println(helpMessage);
                break;
              }
              case "ACTIVE": {
                cl.getActiveChatUsers();
                break;
              }
              default: {
                if (checkMessageSize(msg)) {
                  cl.chatSend(msg); 
                }
              }
            }
          }
          cl.endChat();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
        
    try {
      if (!checkInput(chatID, "chatID", 20)) {
        return;
      }   
      
      if (!checkChatroomReply(cl.startChat(chatID))) {
        return;
      }
      
      printLine("Connected to chatroom " + chatID + ".");
      senderThread.start();
      while (true) {
        String r = cl.chatReceive();
        if (r.equals("QUIT")) {
          break;
        }
        System.out.println("[" + new java.util.Date() + "] " + r);
      }
    } catch (IOException e) {
      logger.severe("Error during chat: " + e.getLocalizedMessage());
    }

    printLine("Chat session is over");
  }

  /**
   * Checks the server response to the user attempt to join a chatroom.
   * @param reply
   * @return true, if user has successfully joined chat
   * @throws IOException
   */
  private boolean checkChatroomReply(String reply) throws IOException{
    switch (reply) {
      case "chatroom_create": { // chatroom does not exist
        return chatroomCreation();
      }
      
      case "chat_success": { // public chatroom exists
        return true;
      }
      
      case "chatroom_password_entry": { // private chatroom exists
        printLine("Please enter the password:");
        reply = cl.sendChatroomPWD(read());      
        return checkChatroomReply(reply);
      }
      
      default: { // unable to join chatroom
        printLine(reply);
        return false;
      }
    }
  }

  private boolean chatroomCreation() {
    printLine("Creating new chatroom..");
    printLine("Select chatroom type: [PUBLIC, PRIVATE]");
    
    ChatroomType type;
    try {
      type = ChatroomType.valueOf(read()); 
    } catch (IllegalArgumentException e) {
      printLine("Invalid chatroom type");
      return false;
    }
    
    switch (type) {      
      case PUBLIC: {
        cl.createPublicChatroom();
        break;
      }
      
      case PRIVATE: {
        String password = null;
        while (password == null) {
          printLine("Enter chatroom password:");
          password = read();    
          if (!checkInput(password, "password", 20)) {
            password = null;
          }
        }
        cl.createPrivateChatroom(password);
        break;
      }
    }
    
    return true;
  }

  /**
   * Closes the connection via socket to the currently connected server. Will write on console and
   * log.
   */
  private void close() {
    try {
      cl.close();
      logger.info("Successfully disconnected");
    } catch (IOException e) {
      printLine("Error during disconnection");
      logger.warning("Error: connection cannot be closed: " + e.getMessage());
    }
  }

  /**
   * Sets the current log level to the log level given by the parameter newLog. Will write on
   * console and log.
   * 
   * @param newlog new log level
   */
  private void changeLogLevel(String[] tokens) {
    try {
      Level level = Level.parse(tokens[1]);
      LogeLevelChange logeLevelChange = LogSetup.changeLoglevel(level);
      printLine(String.format("loglevel changed from: %s to: %s", logeLevelChange.getPreviousLevel(), logeLevelChange.getNewLevel()));
    } catch (IllegalArgumentException ex) {
      printLine("Unknown loglevel");
    }
  }

  /**
   * Prints help message containing all possible commands on the console. Will write on console.
   */
  private static void printHelp() {
    System.out.println("Following commands are possible:");
    System.out.println("connect <address> <port> --> Establishes a TCP connection to the server at the given address and port");
    System.out.println("put <key> <value>        --> Stores the given value and allows future access to it through the provided key");
    System.out.println("put <key>                --> Deletes the value assigned to the given key");
    System.out.println("get <key>                --> Returns the value assigned to the given key");
    System.out.println("chat <chatID>            --> Enters the chatroom with the provided chatID");
    System.out.println("quit                     --> Exits from the application");
    System.out.println("disconnect               --> Disconnects from the connected server");
    System.out.println("logLevel <level>         --> Sets the logger to the specified log level");
    System.out.println("help                     --> Displays this text");
  }
  
  /**
   * Checks that the value <code>value</code> of the parameter <code>name</code> does not contain whitespaces and is not larger than <code>max</code> characters long.
   * @param value
   * @param name
   * @param max
   * @return true, if the value satisifies both conditions
   */
  private boolean checkInput(String value, String name, int max) {
    if (value.contains(" ")) {
      printLine("Whitespaces are not allowed in " + name + ".");
    } else if (value.length() > max) {
      printLine("Maxmimum " + name + " length exceeded");
    } else {
      return true;
    }
    return false;
  }
  
  private void printLine(String msg) {
    System.out.println("Client> " + msg);
  }
}
