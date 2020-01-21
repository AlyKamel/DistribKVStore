package de.tum.i13.server.kv;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Logger;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.server.threadperconnection.ServerRing;
import de.tum.i13.shared.CommandProcessor;

public class KVCommandProcessor implements CommandProcessor {
  public final KVStore kvs;
  private final static Logger logger = Logger.getLogger(Main.class.getName());
  public ServerRing sr;
  private boolean writeLock;

  public KVCommandProcessor(KVStore kvs, ServerRing sr) {
    this.kvs = kvs;
    this.sr = sr;
  }

  @Override
  public String process(String command) {
    if (command.equals("keyrange")) {
      logger.fine("keyrange sent");
      return "keyrange_success " + sr.getKeyRange();
    } else if (command.equals("keyrange_read")) {
      logger.fine("readkeyrange sent");
      return "keyrange_read_success " + sr.getReadKeyRange();
    }

    String reply;
    String[] tokens = command.split("\\s", 3);
    String key = tokens[1];
    
    switch (tokens[0]) {
      case "put": {
        reply = checkWritePermission(key);
        if (reply == null) {
          String value = tokens[2];
          if (value.contains(" ")) {
            value = value.substring(1, value.length() - 1); // removes quotations
            // currently: put mykey "myvalue" -> stores: "myvalue" at mykey
          }
          synchronized (this) {
            kvs.setLookID(0);
            ServerStatus status = kvs.put(key, value);
            reply = "put_" + setupReply(status, key, ServerStatus.ERROR, value);
          }
        }
        break;
      }

      case "get": {
        if (!sr.isReadResponsible(key)) {
          reply = "server_not_responsible";
        } else {
          synchronized (this) {
            setLookID(key);
            String rv = kvs.get(key);
            ServerStatus status2 = (rv == null) ? ServerStatus.ERROR : ServerStatus.SUCCESS;
            reply = "get_" + setupReply(status2, key, ServerStatus.SUCCESS, rv);
          }
        }
        break;
        
      }
      case "delete": {
        reply = checkWritePermission(key);
        if (reply == null) {
          synchronized (this) {
            kvs.setLookID(0);
            ServerStatus status = kvs.delete(key);
            reply = "delete_" + status + " " + key;
          }
        }
        break;
      }

      default: {
        reply = "error: command unrecognized";
      }
    }
    
    logger.fine(reply);
    return reply;
  }

  @Override
  public String connected(InetSocketAddress address, InetSocketAddress remoteAddress) {
    logger.info("New connection: " + remoteAddress);
    return "Connection to KV-storage server established: " + address;
  }

  @Override
  public void connectionClosed(InetAddress remoteAddress) {
    logger.info("Connection ended: " + remoteAddress);
  }

  public void updateServerRing(String newKeyRange) {
    sr.update(newKeyRange);
  }

  public void setWriteLock(boolean flag) {
    writeLock = flag;
  }

  private String setupReply(ServerStatus status, String key, ServerStatus checkStatus, String val) {
    String reply = status + " " + key;
    if (status == checkStatus) {
      reply += " " + val;
    }
    return reply;
  }

  private String checkWritePermission(String key) {
    if (!sr.isCoordinator(key)) {
      return "server_not_responsible";
    }
    if (writeLock) {
      return "server_write_lock";
    }
    return null;
  }
  
  private void setLookID(String key) {
    List<String> servers = sr.getReadResponsibleServers(key);
    int index = servers.indexOf(sr.getServer());
    kvs.setLookID(index);
  }
}
