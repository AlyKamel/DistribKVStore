package de.tum.i13.server.kv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;
import de.tum.i13.server.ecs.ReplicationManager;
import de.tum.i13.server.kv.caching.Cache;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.server.threadperconnection.ServerRing;
import de.tum.i13.shared.ServerUtility;

/**
 * This implements the three basic operations of a KVStore by using an on-disk storage system and
 * also a server-side write-through caching system.
 */
public class KVStore implements KVStorageSystem {

  private final Cache cache;
  private final DiskStore disk;
  private final static Logger logger = Logger.getLogger(Main.class.getName());
  private ReplicationManager repManager = new ReplicationManager(this);

  public KVStore(DiskStore disk, Cache cache) {
    this.disk = disk;
    this.cache = cache;
  }

  /**
   * Stores the given key-value pair in the cache and on the disk. Will write on log.
   * 
   * @param key key to be stored
   * @param value value to be stored
   * @return ServerStatus to indicate if request was successful
   */
  public ServerStatus put(String key, String value) {
    logger.fine("Inserting KEY " + key + " and VALUE " + value + " in cache and disk");
    if (isCoordinator()) {
      repManager.forward(key + " " + value); 
    }
    ServerStatus s1 = cache.put(key, value);
    ServerStatus s2 = disk.put(key, value);
    return ServerStatus.maxValue(s1, s2); // makes sure value is inserted in both
  }

  private boolean isCoordinator() {
    return disk.getLookID() == 0;
  }
  

  /**
   * Tries to retrieve the value of the provided key from the cache. If value is not found, an
   * attempt is made to retrieve the value from the on-disk storage. The cache is updated with the
   * fetched value. Will write on log.
   * 
   * @param key
   * @return RetrievedValue indicating the value of the key provided (if found) and the request
   *         status
   */
  public String get(String key) {
    logger.finest("Looking in CACHE for KEY " + key);
    String value = cache.get(key);
    if (value == null) {
      logger.finest("CACHE MISS for KEY " + key);
      value = disk.get(key);
      if (value == null) {
        logger.finest("STORE MISS for KEY " + key);
        return null;
      }
      logger.finest("CACHE UPDATE for KEY " + key);
      cache.put(key, value);
    }
    return value;
  }

  /**
   * Deletes the provided key and its value from the disk and cache.
   * 
   * @param key
   * @return ServerStatus to indicate if request was successful
   */
  public ServerStatus delete(String key) {
    logger.fine("Deleting KEY " + key + " from CACHE and STORE");
    if (isCoordinator()) {
      repManager.forward(key); 
    }
    ServerStatus s1 = cache.delete(key);
    ServerStatus s2 = disk.delete(key);
    return ServerStatus.maxValue(s1, s2); // makes sure value is deleted in both
  }
  
  public void setLookID(int index) {
    disk.setLookID(index);
  }

  /**
   * Receives all the data another server is sending using
   * {@link #sendData(String, int, ServerRing)}
   * 
   * @param ss used for the communication with other server return void
   */
  public ServerStatus receiveData(ServerSocket ss) throws IOException {
      Socket s = ss.accept();
      BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
      ServerStatus finalStatus = ServerStatus.SUCCESS;
      ServerStatus status;
      int counter = 0;
      String line;
      while ((line = in.readLine()) != null) {
        String[] keyvalue = line.split("\\s+");
        synchronized (this) {
          disk.setLookID(0);
          status = disk.put(keyvalue[0], keyvalue[1]); 
        }
        finalStatus = ServerStatus.maxValue(finalStatus, status);
        counter++;
      }
      logger.finer("Received " + counter + " key-value pairs");
      
      ss.close();
      s.close();
      return finalStatus;
  }

  /**
   * Send data to another server whose address is defined in the parameters. If a ServerRing is
   * provided, this method only sends keys which that server is responsible for.
   * 
   * @param address address of the second server
   * @param port port of the second server return void
   */
  public void sendData(String address, int port, ServerRing sr) throws IOException {
      InetSocketAddress sa = new InetSocketAddress(address, port);
      Socket s = ServerUtility.connectNonstop(sa);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String[] keys;   
      synchronized (this) {
        disk.setLookID(0);
        keys = disk.getAllKeys(); 
      }
      
      int counter = 0;
      for (String k : keys) {
        if (sr == null || sr.isCoordinator(k)) {
          counter++;
          synchronized (this) {
            disk.setLookID(0);
            out.println(k + " " + get(k));
          }
        }
      }
      logger.finer("Sending over " + counter + " key-value pairs");
      out.flush();
      s.close();
      out.close();
  }

  /**
   * Deletes everything in the directory of the disk store
   * 
   * @return void
   */
  public synchronized boolean deleteAll() {
    return disk.deleteAll();
  }

  public ServerStatus deleteRangeData(ServerRing sr) {
    logger.fine("Deleting sent data");
    ServerStatus finalStatus = ServerStatus.SUCCESS;
    String[] keys;
    ServerStatus status;
    synchronized (this) {
      disk.setLookID(0);
      keys = disk.getAllKeys(); 
    }
    for (String k : keys) {
      if (sr.isCoordinator(k)) {
        synchronized (this) {
          disk.setLookID(0);
          status = delete(k); 
        }
        finalStatus = ServerStatus.maxValue(finalStatus, status);
      }
    }
    return finalStatus;
  }
  
  public void setupReplication(int port) {
    repManager.openReplicationSocket(port);
  }
  
  public void setReplica(int replicaNum, InetSocketAddress socketaddress, boolean adding) {
    try {
      repManager.changeReplica(replicaNum, socketaddress, adding);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void changeCoordinator(int coordinatorNum, boolean adding) {
    repManager.changeCoordinator(coordinatorNum, adding);
  }

  public void startReplication(int repPort) {
    try {
      repManager.setup(repPort);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void startupReplica(int replicaNum, InetSocketAddress socketaddress, int myPort) {
    repManager.startupReplica(replicaNum, socketaddress, myPort);
  }
  
  /**
   * Sends all data in directory repID using the given PrintWriter.
   * 
   * @param out PrintWriter used for the sending of the data
   * @param repID ID of the replica in order to work on the corresponding directory
   * */
  public void replicateData(PrintWriter out, int repID) {
    String[] keys;
    synchronized (this) {
      setLookID(repID);
      keys = disk.getAllKeys(); 
    }
    for (String k : keys) {
      synchronized (this) {
        setLookID(repID);
        out.println(k + " " + get(k)); 
      }
      logger.finer("Sending KEY : " + k);
    }
    out.flush();
  }

  /**
   * It deletes every file in the directory defined by repID. Doesn't the directory itself.
   * 
   * @param repID ID of the replica in order to work on the corresponding directory
   * */
  public synchronized void emptyDirectory(int repID) {
  	setLookID(repID);
  	disk.emptySubFolder(); // doesn't delete the folder itself
  }
  
  /**
   * Depending on repID, this methods replaces the directory of replica 1 with the directory of
   * replica 2 or the other way around. All earlier data in the destination directory gets deleted
   * and the source directory gets emptied. The directory defined in repID is the source directory.
   * 
   * @param repID ID of the replica in order to work on the corresponding directory
   * */
  public void replaceReplica(int repID) {
  	if(repID == 1)
  		disk.replaceFolder(1, 2);
  	else if (repID == 2)
  		disk.replaceFolder(2, 1);
  	else {
  		logger.severe("Error @switchDirectory repID = " + repID);
  	}
  }
  
  /**
   * Adds all files from replica 1 with ID = 1 to the original directory (which has ID = 0)
   * */
	public void addToResponsibility() {
		disk.copyFolder(1, 0);
	}

  public void endReplication() {
    try {
      repManager.endReplication();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void chatAppend(String chatID, String msg) {
    msg = "[" + new java.util.Date() + "] " + msg;
    disk.chatAppend(chatID, msg);
  }
}
