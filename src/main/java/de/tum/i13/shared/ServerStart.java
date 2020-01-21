package de.tum.i13.shared;

import static de.tum.i13.shared.LogSetup.setupLogging;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.server.kv.DiskStore;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.caching.Cache;
import de.tum.i13.server.kv.caching.CachingStrategy;
import de.tum.i13.server.threadperconnection.ServerRing;

public class ServerStart {
  
  public static ServerSocket setup(ConfigECS cfg) throws IOException {
    if (cfg.usagehelp) {
      displayHelp();
      System.exit(0);
    }

    setupLogging(cfg.logfile, cfg.loglevel);
    
    final ServerSocket socket = new ServerSocket();
    socket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

    return socket;
  }
  
  public static KVCommandProcessor getCommandProcessor(ConfigServer cfg) {
    CachingStrategy cs = CachingStrategy.valueOf(cfg.cachedisplacement);
    DiskStore disk = new DiskStore(cfg.dataDir.toString());
    Cache cache = new Cache(cs, cfg.cachesize);
    KVStore kvs = new KVStore(disk, cache); 
    ServerRing sr = new ServerRing(new HashRing(), cfg.listenaddr + ":" + cfg.port);
    return new KVCommandProcessor(kvs, sr);
  }

  private static void displayHelp() {
    System.out.println("-p <port>           --> Sets the port of the server (default: 5153)");
    System.out.println("-s <address>        --> Sets the address of the server (default: 127.0.0.1)");
    System.out.println("-b <address>:<port> --> Sets the address of the bootstrap broker (default: clouddatabases.i13.in.tum.de:5153)");
    System.out.println("-d <directory>      --> Sets the directory for files (default: data/)");
    System.out.println("-l <path>           --> Sets the relative path of the logfile (default: echo.log)");
    System.out.println("-ll <logLevel>      --> Sets the logging level (default: INFO)");
    System.out.println("-c <size>           --> Sets the maximum amount of key-value pairs stored in cache");
    System.out.println("-s <strategy>       --> Sets the cache displacement strategy");
    System.out.println("-h                  --> Displays this text");
  }
}
