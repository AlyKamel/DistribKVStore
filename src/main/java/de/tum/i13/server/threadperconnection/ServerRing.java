package de.tum.i13.server.threadperconnection;

import java.util.List;
import de.tum.i13.server.ecs.HashRing;

public class ServerRing {
  private HashRing hr;
  private String server;

  public ServerRing(HashRing hr, String serverAddress) {
    this.hr = hr;
    server = serverAddress.replaceAll(".*/", "");
  }

  /**
   * Getter for the keyrange
   * 
   * @return keyrange
   */
  public String getKeyRange() {
    return hr.getKeyRange();
  }

  /**
   * Getter for the read keyrange
   * 
   * @return read keyrange
   */
  public String getReadKeyRange() {
    return hr.getReadKeyRange();
  }

  /**
   * Checks if the server is the coordinator of the given key
   * 
   * @param key
   * @return true if the server is responsible for write operations on this key, false otherwise
   */
  public boolean isCoordinator(String key) {
    return hr.isCoordinator(server, key);
  }

  /**
   * Finds the coordinator server for the given key
   * 
   * @param key
   * @return server
   */
  public String getCoordinator(String key) {
    return hr.getCoordinator(key);
  }

  /**
   * Checks if the server could store the value for the given key.
   * 
   * @param key
   * @return true if the server is responsible for get operations on this key, false otherwise
   */
  public boolean isReadResponsible(String key) {
    return hr.isReadResponsible(server, key);
  }

  /**
   * Getter for the servers that could be used for read operations on the provided key
   * 
   * @return read responsible servers
   */
  public List<String> getReadResponsibleServers(String key) {
    return hr.getReadResponsibleServers(key);
  }

  /**
   * Updates the hash ring with the provided key range
   * 
   * @param newKeyRange
   */
  public void update(String newKeyRange) {
    hr.setKeyRange(newKeyRange);
  }

  public String getServer() {
    return server;
  }
}
