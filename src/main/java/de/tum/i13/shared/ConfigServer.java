package de.tum.i13.shared;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import picocli.CommandLine;

public class ConfigServer extends ConfigECS {
  @CommandLine.Option(names = "-b", description = "bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153", defaultValue = "clouddatabases.i13.in.tum.de:5153")
  public InetSocketAddress bootstrap;

  @CommandLine.Option(names = "-d", description = "Directory for files", defaultValue = "data/")
  public Path dataDir; 

  @CommandLine.Option(names = "-c", description = "Sets the cachesize, e.g., 100 keys", defaultValue = "16")
  public int cachesize;

  @CommandLine.Option(names = "-s", description = "Sets the cache displacement strategy, FIFO, LRU, LFU", defaultValue = "LRU")
  public String cachedisplacement;

  public static ConfigServer parseCommandlineArgs(String[] args) {
    ConfigServer cfg = new ConfigServer();
    CommandLine.ParseResult parseResult = new CommandLine(cfg).registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter()).parseArgs(args);

    if (!parseResult.errors().isEmpty()) {
      for (Exception ex : parseResult.errors()) {
        ex.printStackTrace();
      }

      CommandLine.usage(new ConfigECS(), System.out);
      System.exit(-1);
    }

      try {
        Files.createDirectories(cfg.dataDir);
      } catch (IOException e) {
        System.out.println("Could not create directory");
        e.printStackTrace();
        System.exit(-1);
      }
    
    
    return cfg;
  }
  
  @Override
  public String toString() {
    return "Config{" + "port=" + port + ", listenaddr='" + listenaddr + '\'' + ", bootstrap="
        + bootstrap + ", dataDir=" + dataDir + ", logfile=" + logfile + ", loglevel='" + loglevel
        + '\'' + ", cachesize=" + cachesize + ", cachedisplacement='" + cachedisplacement + '\''
        + ", usagehelp=" + usagehelp + '}';
  }
  
}
