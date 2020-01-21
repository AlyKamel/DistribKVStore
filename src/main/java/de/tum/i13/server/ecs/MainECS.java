package de.tum.i13.server.ecs;

import static de.tum.i13.shared.ConfigECS.parseCommandlineArgs;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.logging.Logger;
import de.tum.i13.shared.ConfigECS;
import de.tum.i13.shared.ServerStart;

public class MainECS {
  private static ArrayList<Thread> workers = new ArrayList<Thread>();
  private static Logger logger = Logger.getLogger(MainECS.class.getName());
  private static boolean running = true;

  public static void main(String[] args) throws IOException {
    ConfigECS cfg = parseCommandlineArgs(args);
//    cfg.listenaddr = "127.0.0.1"; // for testing
//    cfg.port = 5153; // for testing
    final ServerSocket mainSocket = ServerStart.setup(cfg);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          logger.info("ECS server closing..");
          for (Thread th : workers) {
            th.join();
          }
          running = false;
          mainSocket.close();
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    HashRing hr = new HashRing();

    logger.info("ECS server starting..");
    try {
      while (running) {
        Socket serverSocket = mainSocket.accept(); // new server
        Thread th = new ServerCommThread(serverSocket, hr);
        workers.add(th);
        th.start();
      }
    } catch (SocketException e) {
      if (!running) { // shutdownhook is shutting server down
        logger.info("ECS server shutdown successful");
      } else { 
        logger.severe("ECS server quit unexpectedly: " + e.getMessage());
      }
    }
  }
}
