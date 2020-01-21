package de.tum.i13;

import java.io.IOException;
import java.net.ServerSocket;
import de.tum.i13.server.ecs.MainECS;
import de.tum.i13.server.threadperconnection.Main;

public class Util {

  public static int getFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  public static Thread createServer(String address, int port, String bootstrap) {
    Thread serverThread = new Thread() {
      @Override
      public void run() {
        try {
          while (!this.isInterrupted()) {
            Main.main(new String[] {"-b", bootstrap, "-a", address, "-p", String.valueOf(port),"-d", "data/" + port + "/"});
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };

    return serverThread;
  }
  
  public static Thread createServerWithCachingStrategy(String address, int port, String bootstrap, String cacheStrategy) {
		Thread serverThread = new Thread() {
			@Override
			public void run() {
				try {
					Main.main(new String[] { "-b", bootstrap, "-a", address, "-p", String.valueOf(port), "-d",
							"data/" + port + "/", "-s", cacheStrategy});
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};

		return serverThread;
	}


  public static Thread createECSServer(String address, int port) {
    Thread ecsThread = new Thread() {
      @Override
      public void run() {
        try {
          MainECS.main(new String[] {"-a", address, "-p", String.valueOf(port)});
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };

    return ecsThread;
  }
}
