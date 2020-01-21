package de.tum.i13.server.threadperconnection;

import static de.tum.i13.shared.ConfigServer.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Logger;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.shared.ConfigServer;
import de.tum.i13.shared.ServerStart;
import de.tum.i13.shared.ServerUtility;

public class Main {
  private static KVCommandProcessor cp;
  public static HashRing hr = new HashRing();
  private static ArrayList<ConnectionHandleThread> chtList = new ArrayList<ConnectionHandleThread>();
  private static boolean writeLock = false;
  private final static Object hrLock = new Object();
  private final static Logger logger = Logger.getLogger(Main.class.getName());
  
  public static String kvAddress; // address which clients use to reach the server
  
  public static void main(String[] args) throws IOException {
    ConfigServer cfg = parseCommandlineArgs(args);
    Main m = new Main();
    ServerSocket socket = m.setup(cfg);
    m.start(socket);
  }
  
  private ServerSocket setup(ConfigServer cfg) throws IOException {
//    cfg.port = 0; // for testing
//    cfg.bootstrap = new InetSocketAddress("127.0.0.1", 5153); // for testing
    ServerSocket serverSocket = ServerStart.setup(cfg);
    kvAddress = serverSocket.getLocalSocketAddress().toString().substring(1);

    // for testing
//    cfg.loglevel = "ALL";
//    cfg.dataDir = Paths.get("data/" + serverSocket.getLocalPort() + "/");
//    try {
//      Files.createDirectory(cfg.dataDir);
//    } catch (IOException e) {
//      System.out.println("Could not create directory");
//      e.printStackTrace();
//      System.exit(-1);
//    }
    //

    cfg.port = serverSocket.getLocalPort();
    setupLogging(cfg.logfile, cfg.loglevel);
    cp = ServerStart.getCommandProcessor(cfg);

    ECSCommThread ecsThread = new ECSCommThread(cfg.bootstrap);
    ecsThread.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        logger.info("Server closing..");
        ecsThread.closeServer();
        try {
          ecsThread.join();
          // serverSocket.close();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    // wait for hash ring to get updated before passing it
    synchronized (hrLock) {
      try {
        hrLock.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      cp.updateServerRing(hr.getKeyRange());
    }
    
    return serverSocket;
  }

  private void start(ServerSocket socket) throws IOException {
    logger.info("Server " + kvAddress + " starting..");
    while (true) {  // listen to new clients and open a thread to handle each one of them
      Socket clientSocket = socket.accept();
      cp.updateServerRing(hr.getKeyRange()); // make sure hash ring is up to date
      ConnectionHandleThread th = new ConnectionHandleThread(cp, clientSocket);
      th.setWriteLock(writeLock); // potentially set write lock
      chtList.add(th);
      th.start();
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Communicates with the ECS to always keep the server up to date
   */
  public static class ECSCommThread extends Thread {
    private final Socket ecsSocket;
    private static PrintWriter out;

    public ECSCommThread(InetSocketAddress bootstrap) {
      ecsSocket = new Socket();
      try {
        ecsSocket.connect(bootstrap);
        out = new PrintWriter(new OutputStreamWriter(ecsSocket.getOutputStream()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    
    public static class ServerToECS {
      private static String username;
      private static final Object userLock = new Object();
      
      public static synchronized String addUser(String username) {
        sendMessage("addUser");
        sendMessage(username);
        synchronized (userLock) {
          try {
            userLock.wait(); 
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        return ServerToECS.username;
      }
      
      public static synchronized boolean removeUser(String username) {
        sendMessage("removeUser");
        sendMessage(username);
        synchronized (userLock) {
          try {
            userLock.wait(); 
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        return ServerToECS.username != null;
      }
      
      public static void setUsername(String username) {
        synchronized (userLock) {
          ServerToECS.username = username;
          userLock.notify(); 
        }
      }
    }
    
    @Override
    public void run() {
      sendMessage(kvAddress);

      try (Scanner in = new Scanner(ecsSocket.getInputStream())) {
        readLoop: while (!ecsSocket.isClosed()) {
          String cmd = in.next();
          synchronized (this) {
            switch (cmd) {
              case "receive": {
                int recvPort = Integer.parseInt(in.next());
                receive(recvPort);
                break;
              }

              case "send": {
                setWriteLock(true);
                String sendKvAddress = in.next();
                String sendAddress = ServerUtility.getAddress(sendKvAddress); // address to send to
                int sendPort = Integer.parseInt(in.next()); // port to send to
                boolean closing = in.nextBoolean();
                if (closing) {
                  prepareShutdown(sendAddress, sendPort);
                } else {
                  sendToNewServer(sendKvAddress, sendAddress, sendPort);
                }
                break;
              }

              case "update": {
                String keyrange = in.next();
                updateHashRing(keyrange);
                break;
              }

              case "openReplicationPort": {
                int repPort = Integer.parseInt(in.next());
                cp.kvs.setupReplication(repPort);
                break;
              }

              case "setReplica": {
                int replicaNum = Integer.parseInt(in.next());
                String address = in.next();
                int port = Integer.parseInt(in.next());
                InetSocketAddress socketaddress = new InetSocketAddress(address, port);
                boolean adding = Boolean.parseBoolean(in.next());
                cp.kvs.setReplica(replicaNum, socketaddress, adding);
                break;
              }

              case "setCoordinator": {
                int coordinatorNum = Integer.parseInt(in.next());
                boolean adding = Boolean.parseBoolean(in.next());
                cp.kvs.changeCoordinator(coordinatorNum, adding);
                break;
              }

              case "startReplication": {
                int repPort = Integer.parseInt(in.next());
                cp.kvs.startReplication(repPort);
                break;
              }

              case "startup": {
                int replicaNum = Integer.parseInt(in.next());
                String address = in.next();
                int port = Integer.parseInt(in.next());
                InetSocketAddress socketaddress = new InetSocketAddress(address, port);
                int myPort = Integer.parseInt(in.next());
                cp.kvs.startupReplica(replicaNum, socketaddress, myPort);
                break;
              }

              case "endReplication": {
                cp.kvs.endReplication();
                break;
              }

              case "close": {
                cp.kvs.deleteAll();
                sendMessage("exit");
                break readLoop;
              }
              
              case "ping": {
                sendMessage("pong");
                break;
              }
              
              case "userResult": {
                boolean success = in.nextBoolean();
                String username = in.next();
                if (!success) {
                  username = null;
                }
                ServerToECS.setUsername(username);
                break;
              }
      
              default: {
                throw new IOException("Unknown command received from the ECS");
              }
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    public synchronized void closeServer() {
       sendMessage("closing"); 
    }

    private void receive(int recvPort) throws IOException {
      ServerSocket ss = new ServerSocket();
      String address = ServerUtility.getAddress(kvAddress);
      InetSocketAddress sa = new InetSocketAddress(address, recvPort);
      ss.bind(sa);
      cp.kvs.receiveData(ss);
    }

    private void prepareShutdown(String sendAddress, int sendPort) throws IOException {
      logger.finer("Preparing to shutdown server");
      cp.kvs.sendData(sendAddress, sendPort, null);
      sendMessage("exit");
      cp.kvs.deleteAll();
    }

    private void sendToNewServer(String sendKvAddress, String sendAddress, int sendPort) throws IOException {
      logger.finer("Sending data to the new server");
      ServerRing sr = new ServerRing(hr, sendKvAddress);
      cp.kvs.sendData(sendAddress, sendPort, sr);
      setWriteLock(false);
      cp.kvs.deleteRangeData(sr);
    }

    private void updateHashRing(String newRange) {
      logger.finest("Updating key range");
      synchronized (hrLock) {
        hr.setKeyRange(newRange);
        hrLock.notify();
      }
      synchronized (hr) {
        chtList.forEach(t -> t.updateSR(hr.getKeyRange())); // send update to all client-handling threads 
      }
    }

    private void setWriteLock(boolean status) {
      writeLock = status;
      chtList.forEach(t -> t.setWriteLock(writeLock));
    }

    private static void sendMessage(String msg) {
      out.println(msg);
      out.flush();
    }
  }
}
