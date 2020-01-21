package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import picocli.CommandLine;

public class ConfigECS {
  @CommandLine.Option(names = "-p", description = "sets the port of the server", defaultValue = "5153")
  public int port;

  @CommandLine.Option(names = "-a", description = "which address the server should listen to", defaultValue = "127.0.0.1")
  public String listenaddr;

  @CommandLine.Option(names = "-l", description = "Logfile", defaultValue = "ecs.log")
  public Path logfile;

  @CommandLine.Option(names = "-ll", description = "Loglevel", defaultValue = "INFO")
  public String loglevel;

  @CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
  public boolean usagehelp;

  public static ConfigECS parseCommandlineArgs(String[] args) {
    ConfigECS cfg = new ConfigECS();
    CommandLine.ParseResult parseResult = new CommandLine(cfg).registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter()).parseArgs(args);

    if (!parseResult.errors().isEmpty()) {
      for (Exception ex : parseResult.errors()) {
        ex.printStackTrace();
      }

      CommandLine.usage(new ConfigECS(), System.out);
      System.exit(-1);
    }

    return cfg;
  }

  @Override
  public String toString() {
    return "Config{" + "port=" + port + ", listenaddr='" + listenaddr + '\'' + ", logfile="
        + logfile + ", loglevel='" + loglevel + ", usagehelp=" + usagehelp + '}';
  }
}

