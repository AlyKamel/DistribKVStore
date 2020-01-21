package de.tum.i13.server.ecs;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import de.tum.i13.shared.ServerUtility;

public class ECSLibrary {
  private final PrintWriter out;
  private static int eventPort;
  
  public ECSLibrary(PrintWriter out) {
    this.out = out;
  }

  /**
   * Sends the newest version of metadata to the server
   */
  public synchronized void updateServer(String keyRange) {
    out.println("update");
    out.println(keyRange);
    out.flush();
  }
  
  public synchronized void startReplication(int repPort) {
    out.println("startReplication");
    out.println(repPort);
    out.flush();
  }

  public synchronized void startup(int num, InetSocketAddress cooAddress, int thisPort) {
    out.println("startup");
    out.println(num);
    out.println(cooAddress.getHostString());
    out.println(cooAddress.getPort());
    out.println(thisPort);
    out.flush();
  }
  
  public synchronized void openReplicationPort(int repPort) throws IOException {
    out.println("openReplicationPort");
    out.println(repPort);
    out.flush();
  }
  
  /**
   * Informs the server the port on which it should receive data
   * 
   * @param port
   */
  public synchronized void sendSenderPort() {
    out.println("receive");
    out.println(eventPort);
    out.flush();
  }

  /**
   * Informs the server the address to which it should send data
   * 
   * @param address
   * @param port
   * @param sendAll true, if the server is shutting down and all data has to be sent, false
   *        otherwise
   */
  public synchronized void sendReceiverAddress(String address, boolean sendAll) {
    out.println("send");
    out.println(address);
    out.println(eventPort);
    out.println(sendAll);
    out.flush();
  }
  
  public synchronized void setReplica(int replicaNum, InetSocketAddress sa, boolean adding) {
    out.println("setReplica");
    out.println(replicaNum);
    out.println(sa.getHostString());
    out.println(sa.getPort());
    out.println(adding);
    out.flush();
  }
  
  public synchronized void setCoordinator(int coordinatorNum, boolean adding) {
    out.println("setCoordinator");
    out.println(coordinatorNum);
    out.println(adding);
    out.flush();
  }

  public synchronized void close() {
    out.println("close");
    out.flush();
  } 

  public synchronized void endReplication() {
    out.println("endReplication");
    out.flush();
  }
  
  public synchronized void ping() {
    out.println("ping");
    out.flush();
  }

  public void setFreeEventPort(String address) throws IOException {
    eventPort = ServerUtility.getFreePort(address);
  }

  public int getEventPort() {
    return eventPort;
  }

  public synchronized void userResult(boolean found, String username) {
    out.println("userResult");
    out.println(found);
    out.println(username);
    out.flush();
  }
}
