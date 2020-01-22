package de.tum.i13.server.ecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Logger;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.ServerUtility;

/**
 * This class takes care of the replication process. It is responsible for forwarding every put
 * operation to the replicas, and keeping its replicated data up to date with the respective
 * coordinators.
 */
public class ReplicationManager {

  private KVStore kvs;
  private ServerSocket ss;
  private ReplicationThread[] threads = new ReplicationThread[2];
  private PrintWriter[] replicaWriters = new PrintWriter[2];
  private final static Logger logger = Logger.getLogger(Main.class.getName());

  public ReplicationManager(KVStore kvs) {
    this.kvs = kvs;
  }

  // coordinator methods
  /**
   * Called by a server when one of its replicas has to be changed due to a server starting or
   * shutting down. This method switches the order of replicas if required then calls
   * {@link #ReplicationManager setReplica(int, InetSocketAddress)}.
   * 
   * @param replicaNum number of the new replica
   * @param sa socket address of the new replica
   * @param adding true if the method is called due to a server starting, false if it is called due
   *        to a server shutting down
   * @throws IOException
   */
  public void changeReplica(int replicaNum, InetSocketAddress sa, boolean adding) throws IOException {
    if (adding || replicaNum == 1) {
      closeWriter(1);
      if (replicaNum == 0) {
        replicaWriters[1] = replicaWriters[0];
        logger.fine("Connected to replica 1");
      }
    } else {
      closeWriter(0);
      replicaWriters[0] = replicaWriters[1];
      logger.fine("Connected to replica " + replicaNum);
      replicaNum = 1;
    }
    
    setReplica(replicaNum, sa);
    kvs.replicateData(replicaWriters[replicaNum], 0);
  }

  /**
   * Used to set one replica of the server. After the replica has been reached, the replica number
   * is sent to it so that it can set up the coordinator. The replicated data is then sent to the
   * new replica.
   * 
   * @param replicaNum number of the new replica
   * @param sa socket address of the new replica
   * @throws IOException
   */
  private Socket setReplica(int replicaNum, InetSocketAddress sa) throws IOException {
    Socket s = ServerUtility.connectNonstop(sa);
    logger.fine("Connected to replica " + replicaNum);
    replicaWriters[replicaNum] = new PrintWriter(s.getOutputStream());
    replicaWriters[replicaNum].println(replicaNum);
    replicaWriters[replicaNum].flush();
    return s;
  }

  /**
   * Used to send data to replicas. Called by the KVStore after a successful put or delete operation. The parameter
   * <code>cmd</code> contains either a key and value seperated by a space for a put operation or just the key for a delete operation.
   * 
   * @param cmd
   */
  public void forward(String cmd) {
    for (PrintWriter out : replicaWriters) {
      if (out != null) {
        out.println(cmd);
        out.flush();
      }
    }
  }

  /**
   * Closes the writer dedicated to the replica <code>replicaNum</code> if it is set up.
   * 
   * @param replicaNum
   */
  private void closeWriter(int replicaNum) {
    if (replicaWriters[replicaNum] != null) {
      replicaWriters[replicaNum].close();
    }
  }


  // replica methods
  /**
   * This method is called by a new server to set up both of its coordinators and start them up. The
   * server socket used to communicate with coordinators is set up.
   * 
   * @param port port that the coordinators are using to reach this server
   */
  public void openReplicationSocket(int port) {
    try {
      ss = new ServerSocket(port);
      logger.finest("Waiting on port " + port + " for coordinators");
      for (int i = 0; i <= 1; i++) {
        Socket s = ss.accept();
        int replicaNum = s.getInputStream().read() - '0';
        threads[replicaNum] = new ReplicationThread(s, replicaNum);
        threads[replicaNum].start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Called by a running server when one of its coordinators has to be changed due to a server
   * starting or shutting down. This method switches the order of coordinators if required then
   * waits for the new coordinator to reach out.
   * 
   * @param coordinatorNum number of the new coordinator
   * @param adding true if the method is called due to a server starting, false if it is called due
   *        to a server shutting down
   * @throws IOException
   */
  public void changeCoordinator(int coordinatorNum, boolean adding) {
    try {
      logger.finest("Changing the coordinator");
      if (adding || coordinatorNum == 1) {
        closeReplicationThread(1);
        if (coordinatorNum == 0) {
          switchReplicationThreads(0);
          logger.fine("Established connection to coordinator 1");
        }
      } else {
        addToResponsibility();
        closeReplicationThread(0);
        switchReplicationThreads(1);
        logger.fine("Established connection to coordinator 0");
        coordinatorNum = 1;
      }

      Socket s = ss.accept();
      s.getInputStream().read(); // unimportant
      threads[coordinatorNum] = new ReplicationThread(s, coordinatorNum);
      threads[coordinatorNum].start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Called when the predecessor of the server has closed. The server is now the coordinator for all
   * the data of its closed predecessor, so it should transfer the keys and send them to its
   * replicas.
   */
  private void addToResponsibility() {
    kvs.addToResponsibility();
    logger.fine("Sending new data to replicas");
    for (PrintWriter out : replicaWriters) {
      if (out != null) {
        kvs.replicateData(out, 1);
      }
    }
  }

  /**
   * Closes the thread dedicated to coordinator <code>coordinatorNum</code> if it is set up and
   * deletes all coordinator data from the server.
   * 
   * @param coordinatorNum
   */
  private void closeReplicationThread(int coordinatorNum) throws IOException {
    if (threads[coordinatorNum] != null) {
      threads[coordinatorNum].close();
      kvs.emptyDirectory(coordinatorNum + 1);
    }
  }

  /**
   * Used to change the position of coordinator <code>coordinatorNum</code>.
   * 
   * @param coordinatorNum
   */
  private void switchReplicationThreads(int coordinatorNum) {
    int otherNum = (coordinatorNum + 1) % 2;
    threads[otherNum] = threads[coordinatorNum];
    kvs.replaceReplica(coordinatorNum + 1);
  }


  // replication setup methods
  /**
   * Called by the third server (moderator) to join the hash ring. The moderator is responsible for
   * the communication between the three servers. It receives the address of the leader and sends it
   * over to the follower, while also using it to establish a connection to the leader.
   * 
   * @param port
   * @throws IOException
   * @throws InterruptedException
   */
  public void setup(int port) throws IOException, InterruptedException {
    ss = new ServerSocket(port);
    BufferedReader[] brs = new BufferedReader[2];
    PrintWriter[] outs = new PrintWriter[2];

    // setup coordinators and establish communication to both servers
    logger.finest("Waiting on port " + port + " for coordinators");
    for (int i = 0; i <= 1; i++) {
      Socket s = ss.accept();
      BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
      int replicaNum = br.read() - '0';
      brs[replicaNum] = br;
      outs[replicaNum] = new PrintWriter(s.getOutputStream());
      threads[replicaNum] = new ReplicationThread(s, replicaNum);
    }

    // inform servers to start the next step
    for (PrintWriter out : outs) {
      out.println("y");
      out.flush();
    }

    // setup replicas and inform the follower server of the leaders address
    for (int i = 1; i >= 0; i--) {
      String address = readNoNull(brs[i]);
      port = Integer.parseInt(readNoNull(brs[i]));
      PrintWriter out = outs[(i + 1) % 2];
      sendSocketAddress(out, address, port);
      InetSocketAddress sa = new InetSocketAddress(address, port);
      setReplica((i + 1) % 2, sa);
      threads[i].start();
    } 
    
    for (int i = 0; i<=1; i++) {
      kvs.replicateData(replicaWriters[i], 0); 
    }
  }
  
  private String readNoNull(BufferedReader br) throws IOException {
    String res = br.readLine();
    while (res.isBlank()) {
      res = br.readLine();
    }
    return res;
  }

  /**
   * Called by both servers in the hash ring when a third one (moderator) joins. Sets the replica
   * for the moderator first, then each server takes over as the leader while the other is in the
   * follower position.
   * 
   * @param replicaNum
   * @param socketAddress
   * @param myPort
   */
  public void startupReplica(int replicaNum, InetSocketAddress socketAddress, int myPort) {
    try {
      Socket s = setReplica(replicaNum, socketAddress);
      BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
      readNoNull(br);
      if (replicaNum == 1) {
        leader(replicaWriters[1], myPort);
        follower(br, 0);
      } else {
        follower(br, 1);
        leader(replicaWriters[0], myPort);
      }     
      for (PrintWriter out : replicaWriters) {
        kvs.replicateData(out, 0); 
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Used to setup coordinators. A server socket is opened and its address gets sent to both
   * followers through the moderator
   * 
   * @param out
   * @throws IOException
   */
  private void leader(PrintWriter out, int port) throws IOException {
    String address = InetAddress.getLocalHost().getHostAddress();
    sendSocketAddress(out, address, port);
    openReplicationSocket(port);
  }

  /**
   * Used to setup replicas, the address of which is provided by the moderator.
   * 
   * @param br
   * @param num
   * @throws IOException
   */
  private void follower(BufferedReader br, int num) throws IOException {
    String address = br.readLine();
    int port = Integer.parseInt(br.readLine());
    InetSocketAddress sa = new InetSocketAddress(address, port);
    setReplica(num, sa);
  }

  private void sendSocketAddress(PrintWriter out, String address, int port) {
    out.println(address);
    out.println(port);
    out.flush();
  }

  /**
   * Used when a server is shutting down or when replication is no longer active to halt the
   * replication process.
   * 
   * @throws IOException
   */
  public void endReplication() throws IOException {
    logger.info("Ending connection to coordinators and replicas");
    for (int i = 0; i <= 1; i++) {
      closeReplicationThread(i);
      closeWriter(i);
    }
    ss.close();
  }

  private class ReplicationThread extends Thread {
    private final int coordinatorNum;
    private final Socket s;
    private boolean running = true;

    public ReplicationThread(Socket s, int coordinatorNum) {
      logger.fine("Established connection to coordinator " + coordinatorNum);
      this.coordinatorNum = coordinatorNum;
      this.s = s;
    }

    @Override
    public void run() {
      try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
        String line;
        while (running && (line = in.readLine()) != null) {
          if (line.length() != 0) {
          String[] cmd = line.split("\\s", 2);
            synchronized (kvs) {
              kvs.setLookID(coordinatorNum + 1);
              if (cmd.length == 2) {
                kvs.put(cmd[0], cmd[1]); 
              }
              else {
                kvs.delete(cmd[0]);
              }
            }
          }
        }
      } catch (SocketException e) {
        if (running) {
          logger.warning("ReplicationThread closed unexpectedly: " + e.getMessage());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void close() throws IOException {
      running = false;
      s.close();
    }
  }
}
