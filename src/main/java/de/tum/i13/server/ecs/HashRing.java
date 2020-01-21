package de.tum.i13.server.ecs;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

public class HashRing {

  private final TreeMap<String, String> buf;
  private MessageDigest md5;
  private String keyRange;
  private String readKeyRange;

  // constructors
  /**
   * Creates a new empty hash ring
   */
  public HashRing() {
    buf = new TreeMap<String, String>();
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) { // nothing to handle
    }
  }

  /**
   * Creates a new hash ring and uses the provided keyRange to setup the ring
   * 
   * @param keyRange
   */
  public HashRing(String keyRange) {
    this();
    this.keyRange = keyRange;
    buildReadKeyRange();
    updateBuf();
  }



  // setting up
  /**
   * Setter for the keyrange with respect to given new keyrange
   * 
   * @param newKeyRange
   */
  public synchronized void setKeyRange(String newRange) {
    keyRange = newRange;
    updateBuf();
    buildReadKeyRange();
  }

  public synchronized void setReadKeyRange(String newRange) {
    readKeyRange = newRange;
    keyRange = newRange;
    updateBuf();
    buildKeyRange();

  }

  /**
   * updates the keyrange
   */
  private synchronized void buildKeyRange() {
    if (buf.isEmpty()) {
      keyRange = "";
      return;
    }
    BigInteger from = new BigInteger(buf.lastKey(), 16).add(BigInteger.ONE);
    if (from.shiftRight(128).compareTo(BigInteger.ONE) == 0) { // is max value (FFF...F)
      from = BigInteger.ZERO;
    }
    StringBuilder keyRange = new StringBuilder();
    for (Entry<String, String> e : buf.entrySet()) {
      keyRange.append(String.format("%032x,%s,%s;", from, e.getKey(), e.getValue()));
      from = new BigInteger(e.getKey(), 16).add(BigInteger.ONE);
    }
    this.keyRange = keyRange.toString();
  }

  public synchronized void buildReadKeyRange() {
    if (buf.size() < 3) {
      readKeyRange = keyRange;
      return;
    }
    String[] ranges = keyRange.split(";");
    String[] newranges = new String[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      String range = ranges[i];
      String rangeWithoutFrom = "," + range.split(",", 2)[1];
      int twobefore = (i - 2 + ranges.length) % ranges.length;
      newranges[i] = ranges[twobefore].split(",")[0] + rangeWithoutFrom;
    }
    readKeyRange = String.join(";", newranges);
  }

  /**
   * Uses the keyRange to setup the ring
   */
  public synchronized void updateBuf() {
    buf.clear();
    String[] rangeAll = keyRange.split(";");
    for (String l : rangeAll) {
      String server = l.split(",")[2];
      buf.put(hash(server), server);
    }
  }



  // getters
  /**
   * Getter for the keyrange
   * 
   * @return keyrange
   */
  public String getKeyRange() {
    return keyRange;
  }

  public String getReadKeyRange() {
    return readKeyRange;
  }

  /**
   * Getter for the keyrange of the respective server
   * 
   * @param server
   * @return range of the respective server
   */
  public String getKeyRange(String server) {
    return getRange(server, false);
  }

  /**
   * Getter for the keyrange of the respective servers for get request
   * 
   * @param server
   * @return range of the respective server
   */
  public String getReadKeyRange(String server) {
    return getRange(server, true);
  }

  private String getRange(String server, boolean read) {
    String[] rangeAll = (read ? readKeyRange : keyRange).split(";");
    for (String range : rangeAll) {
      if (range.endsWith(server)) {
        return range;
      }
    }
    return null;
  }

  /**
   * returns the successor of the given server
   * 
   * @param server
   * @return successor server
   */
  public String getSuccessor(String server) {
    if (buf.size() < 2 || server == null) {
      return null;
    }
    String res;
    return (res = buf.higherKey(hash(server))) == null ? buf.firstEntry().getValue() : buf.get(res);
  }

  public String getPredecessor(String server) {
    if (buf.size() < 2) {
      return null;
    }
    String res;
    return (res = buf.lowerKey(hash(server))) == null ? buf.lastEntry().getValue() : buf.get(res);
  }

  /**
   * Gets the servers that contain a replica (are successors) of the provided server.
   * 
   * @param server
   * @return
   */
  public ArrayList<String> getReplicas(String server) {
    if (!replicationActive()) {
      return null;
    }
    ArrayList<String> m = new ArrayList<String>();
    m.add(getSuccessor(server));
    m.add(getSuccessor(m.get(0)));
    return m;
  }

  /**
   * Gets the servers that the provided server possesses a replica to (predecessors)
   * 
   * @param server
   * @return
   */
  public ArrayList<String> getCoordinators(String server) {
    if (!replicationActive()) {
      return null;
    }
    ArrayList<String> m = new ArrayList<String>();
    m.add(getPredecessor(server));
    m.add(getPredecessor(m.get(0)));
    return m;
  }


  // responsibility
  public boolean isCoordinator(String server, String key) {
    return responsible(server, key, false);
  }

  /**
   * finds the responsible server for the given key
   * 
   * @param key
   * @return server
   */
  public String getCoordinator(String key) {
    for (String server : buf.values()) {
      if (isCoordinator(server, key)) {
        return server;
      }
    }
    return null;
  }

  public boolean isReadResponsible(String server, String key) {
    return responsible(server, key, true);
  }

  /**
   * finds the responsible servers for the given key and returns them in the order: coordinator,
   * replica 1, replica 2
   * 
   * @param key
   * @return server
   */
  public ArrayList<String> getReadResponsibleServers(String key) {
    ArrayList<String> servers = new ArrayList<String>();
    servers.add(getCoordinator(key));
    if (replicationActive()) {
      servers.add(getSuccessor(servers.get(0)));
      servers.add(getSuccessor(servers.get(1)));
    }
    return servers;
  }

  /**
   * checks if the server is responsible for the given key
   * 
   * @param server
   * @param key
   * @return true if server is responsible for the key otherwise false
   */
  private boolean responsible(String server, String key, boolean read) {
    String range[] = (read ? getReadKeyRange(server) : getKeyRange(server)).split(",");
    String k = hash(key);
    String from = range[0];
    String to = range[1];

    if (to.compareTo(from) >= 0) {
      return k.compareTo(from) >= 0 && k.compareTo(to) <= 0;
    }
    return k.compareTo(from) >= 0 || k.compareTo(to) <= 0;
  }



  // map operations
  /**
   * adds server to hashring
   * 
   * @param server
   */
  public synchronized void addServer(String server) {
    String serverHash = hash(server);
    buf.put(serverHash, server);
    buildKeyRange();
    buildReadKeyRange();
  }

  /**
   * removes server from the hashring
   * 
   * @param server
   */
  public synchronized void removeServer(String server) {
    String serverHash = hash(server);
    try {
      buf.remove(serverHash);
    } catch (NullPointerException e) {
      return; // won't do anything if server is not in hash ring
    }
    buildKeyRange();
    buildReadKeyRange();
  }

  /**
   * checks whether ring is empty
   * 
   * @return true if the ring is empty else false
   */
  public boolean isEmpty() {
    return buf.isEmpty();
  }

  /**
   * checks if replication is currently active
   * 
   * @return true if replication is active
   */
  public boolean replicationActive() {
    return buf.size() >= 3;
  }

  /**
   * checks if replication has just been switched on or off.
   * 
   * @return true if replication is now active or inactive due to the most recent hash ring change
   */
  public boolean replicationSwitch() {
    return buf.size() == 3;
  }

  /**
   * checks if only one server is in the hash ring
   * 
   * @return true if only one server is online
   */
  public boolean onlyOneServer() {
    return buf.size() == 1;
  }



  // hashing
  /**
   * hash function for strings
   * 
   * @param string
   * @return hashed string
   */
  public String hash(String k) {
    byte[] digest = md5.digest(k.getBytes());
    StringBuilder hexString = new StringBuilder();
    for (byte b : digest) {
      hexString.append(String.format("%02x", b));
    }
    return hexString.toString();
  }
}

