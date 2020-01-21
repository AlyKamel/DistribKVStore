package de.tum.i13.server.ecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;
import de.tum.i13.shared.ServerUtility;

/**
 * Is responsible for one server to keep it always updated
 */
public class ServerCommThread extends Thread {
  private static final Logger logger = Logger.getLogger(MainECS.class.getName());
  private String kvAddress;
  private final Socket serverSocket;
  private BufferedReader in;
  private ECSLibrary lib;
  private final HashRing hr;
  
  private static enum Event {OPEN, CLOSE, UPDATE};
  private static volatile Event currentEvent;
  private static volatile String eventAddress;
  private static volatile HashMap<String, InetSocketAddress> repList;
  private static volatile CloseLock closeLock;
  private static volatile HashSet<String> users = new HashSet<String>();

  public ServerCommThread(Socket serverSocket, HashRing hr) throws IOException {
    this.hr = hr;
    synchronized (hr) {
      if (repList == null) {
        repList = new HashMap<String, InetSocketAddress>();
        closeLock = new CloseLock();
      }
    }
    this.serverSocket = serverSocket;
    PrintWriter out = new PrintWriter(new OutputStreamWriter(serverSocket.getOutputStream()));
    in = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
    kvAddress = in.readLine();
    lib = new ECSLibrary(out);
  }

  @Override
  public void run() {
    new SignalThread().start();

    try {
      addServer();
    } catch (IOException e) {
      logger.warning("Error during adding server " + kvAddress);
    }

    while (true) {
      synchronized (hr) {
        waitForEvent();

        if (serverSocket.isClosed()) { // server has shutdown
          break;
        }

        switch (currentEvent) {
          case OPEN: {
            updateServer();
            openAction();
            break;
          }

          case CLOSE: {
            updateServer();
            closeAction();
            break;
          }

          case UPDATE: {
            closeLock.unlock();
            updateServer();
            break;
          }
        }
      }
    }

  }

  /**
   * This method integrates the new server into the system. It is added into the hash ring, has its
   * coordinators and replicas setup and receives its share of the data.
   * 
   * @throws IOException
   */
  private void addServer() throws IOException {
    synchronized (hr) {
      logger.info("Server " + kvAddress + " connected");
      eventAddress = kvAddress;
      hr.addServer(kvAddress);
      updateServer();

      String address = InetAddress.getLocalHost().getHostAddress();
      lib.setFreeEventPort(address);
      int repPort = addReplicationPort(address);

      if (!hr.onlyOneServer()) { // data transfer is required
        lib.sendSenderPort();
      }
      if (hr.replicationSwitch()) {
        lib.startReplication(repPort);
      } else if (hr.replicationActive()) {
        lib.openReplicationPort(repPort);
        ArrayList<String> replicas = hr.getReplicas(eventAddress);
        lib.setReplica(0, repList.get(replicas.get(0)), true);
        lib.setReplica(1, repList.get(replicas.get(1)), true);
      }

      setEvent(Event.OPEN);
    }
  }

  /**
   * Called when a server has started. Coordinators of the new server have to adjust their replicas
   * and replicas of the new server have to adjust their coordinators. The direct successor also has
   * to send data to the new server.
   */
  private void openAction() {
    if (kvAddress.equals(hr.getSuccessor(eventAddress))) {
      lib.sendReceiverAddress(eventAddress, false);
    }
    
    int index;
    if (hr.replicationSwitch()) {
      if ((index = hr.getCoordinators(eventAddress).indexOf(kvAddress)) != -1) {
        lib.startup(index, repList.get(eventAddress), repList.get(kvAddress).getPort());
      }
    }

    else if (hr.replicationActive()) {   
      if ((index = hr.getCoordinators(eventAddress).indexOf(kvAddress)) != -1) {
        lib.setReplica(index, repList.get(eventAddress), true);
      }      
      if ((index = hr.getReplicas(eventAddress).indexOf(kvAddress)) != -1) {
        lib.setCoordinator(index, true);
      }
    }
  }

  /**
   * Called when a server has shut down. Coordinators of the closed server have to adjust their
   * replicas and replicas of the closed server have to adjust their coordinators. The direct
   * successor also has to receive data from the closed server.
   */
  private void closeAction() {
    if (hr.replicationSwitch()) {
      lib.endReplication();
      if (kvAddress.equals(hr.getSuccessor(eventAddress))) {
        lib.sendSenderPort();
      }
    }

    else if (hr.replicationActive()) {
      int index;
      if ((index = hr.getCoordinators(eventAddress).indexOf(kvAddress)) != -1) {
        String replica = hr.getReplicas(eventAddress).get((index+1) % 2);
        lib.setReplica(index, repList.get(replica), false);
      }  
      if ((index = hr.getReplicas(eventAddress).indexOf(kvAddress)) != -1) {
        lib.setCoordinator(index, false);
      }
    }

    else if (kvAddress.equals(hr.getSuccessor(eventAddress))) {
      lib.sendSenderPort();
    }
  }

  /**
   * Finds an empty port on the provided address and assigns that port as a replication port for it.
   * 
   * @param address
   * @returns the new replication port
   * @throws IOException
   */
  private int addReplicationPort(String address) {
    int repPort = -1;
    try {
      do {
        repPort = ServerUtility.getFreePort(address);
      } while (repPort == lib.getEventPort());
      InetSocketAddress socketaddress = new InetSocketAddress(address, repPort);
      repList.put(kvAddress, socketaddress);
    } catch (IOException e) {
      logger.severe("Unable to assign replication port");
    }
    return repPort;
  }

  /**
   * Waits until a thread informs it that the hash ring has changed and it needs to update.
   * Triggered by {@link #setEvent(Event)}.
   */
  private void waitForEvent() {
    try {
      hr.wait();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Sends the newest version of metadata to the server
   */
  private void updateServer() {
    lib.updateServer(hr.getKeyRange());
  }

  /**
   * Informs other servers that the hash ring has changed and they need to update accordingly
   * 
   * @param event describes whether a new server has started (<code>OPEN</code>), a server has shut
   *        down (<code>CLOSE</code>) or a regular update is needed (<code>UPDATE</code>)
   */
  private void setEvent(Event event) {
    currentEvent = event;
    hr.notifyAll();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////



  /**
   * Receives signal from server that is is ready to close or receive data
   */
  private class SignalThread extends Thread {
    private boolean running = true;
    private Thread pingThread;
    private final Timer pinger = new Timer();
    
    public SignalThread() {
      pinger.scheduleAtFixedRate(new PingTask(), 1000, 1000);
    }

    @Override
    public void run() {
      try {
        readLoop: while (!serverSocket.isClosed()) {
          String line = in.readLine();
          switch (line) {
            case "closing": {
              logger.info("Server " + kvAddress + " closing..");
              pinger.cancel();
              closeLock.lock();
              synchronized (hr) {
                if (hr.onlyOneServer()) {
                  break readLoop;
                } else {
                  transferData();
                }
              }
              lib.close();
              break;
            }

            case "exit": {
              running = false;
              break readLoop;
            }

            case "pong": {
              if (pingThread != null) {
                pingThread.interrupt(); 
              }
              break;
            }
            
            case "addUser": {
              String username = in.readLine();
              synchronized (users) {
                if (username.equals("QUIT")) {
                  username = "u_" + users.size();
                }
                boolean success = users.add(username);
                if (username.startsWith("BOT")) {
                  success = true;
                }
                lib.userResult(success, username);
              }
              break;
            }
            
            case "removeUser": {
              String username = in.readLine();
              boolean success = users.remove(username);
              lib.userResult(success, username); 
              break;
            }
            
            default: {
              throw new IOException("Unknown command received from server");
            }
          }
        }
      } catch (SocketException e) {
        if (running) {
          logger.warning("ServerCommThread of server " + kvAddress + " quit unexpectedly: " + e.getMessage());
        }
      } catch (IOException e) {
        logger.severe("Error with ServerCommThread of server " + kvAddress + ": " + e.getMessage());
      }
      
      lib.close();
      repList.remove(kvAddress);
      synchronized (hr) {
        hr.removeServer(kvAddress);
        setEvent(Event.UPDATE);
      }
      
      try {
        serverSocket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private void transferData() throws IOException {
      eventAddress = kvAddress;
      if (!hr.replicationActive() || hr.replicationSwitch()) {
        String successor = hr.getSuccessor(eventAddress);
        lib.setFreeEventPort(ServerUtility.getAddress(successor));
        lib.sendReceiverAddress(successor, true);
      } else {
        lib.endReplication();
        lib.close();
      }
      setEvent(Event.CLOSE);
    }

    private void shutdown() throws IOException {
      logger.info("Closing server " + kvAddress + " due to unresponsiveness");
      pinger.cancel();
      running = false;
      
      eventAddress = kvAddress;
      String successor = hr.getSuccessor(eventAddress);
      if (successor != null) {
        lib.setFreeEventPort(ServerUtility.getAddress(successor));      
        synchronized (hr) {
          setEvent(Event.CLOSE); 
        }
      }
      
      lib.close();
      serverSocket.close();
    }


    private class PingTask extends TimerTask {     
      @Override
      public void run() {
        if (pingThread == null) {
          pingThread = Thread.currentThread(); 
        }
        lib.ping();
        try {
          Thread.sleep(1300);
          shutdown();
        } catch (InterruptedException e) { // nothing to handle, server is alive
        } catch (IOException e) {
          logger.warning("Error with pinging server " + kvAddress + ": " + e.getMessage());
        }
      }
    }
  }
}
