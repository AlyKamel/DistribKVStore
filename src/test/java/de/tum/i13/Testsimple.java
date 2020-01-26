package de.tum.i13;

import static de.tum.i13.Util.createECSServer;
import static de.tum.i13.Util.createServer;
import static de.tum.i13.shared.ServerUtility.getFreePort;
import static de.tum.i13.shared.ServerUtility.getSocketAddress;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.tum.i13.server.ecs.MainECS;
import de.tum.i13.server.threadperconnection.Main;

public class Testsimple {
  
	int cCount = 1; // number of clients threads

	private static final int serverCount = 2;
  private static int[] ports = new int[serverCount];
  private static final String address = "127.0.0.1";
  private static final Thread[] servers = new Thread[serverCount];
	
  @BeforeAll
  public static void setup() throws IOException, InterruptedException{
    Logger.getLogger(MainECS.class.getName()).setLevel(Level.ALL);
    Logger.getLogger(Main.class.getName()).setLevel(Level.ALL);

    int ecsport = getFreePort(address);
    String bootstrap = getSocketAddress(address, ecsport);
    Thread ecs = createECSServer(address, ecsport);
    ecs.start();
    
    for (int i = 0; i < serverCount; i++) {
      ports[i] = getFreePort(address);
      servers[i] = createServer(address, ports[i], bootstrap);
      servers[i].start();
      Thread.sleep(3000);
    }
 
    Thread.sleep(2000);  
  } 
    
 @Test
 public void TestSimple() {
	 System.out.println("###START###\n");
	 
	 String addr = address; //server
	 int port = ports[0]; //server
	 String path = "C:\\Users\\Ricardo\\Desktop\\Cloud DB\\milestone5additional\\src\\test\\java\\de\\tum\\i13\\inputs";
	 ClientThread[] clients = new ClientThread[cCount];

	 //in the beginning of the test
	 for(int i = 0; i < cCount; i++) {
		 clients[i] = new ClientThread(""+i, addr, port, path+"\\"+i+".txt"); 
	 }
	 
	 System.out.println("\nStart Clients:\n");
	 for(int i = 0; i < cCount; i++) {
		 clients[i].start();
		 System.out.println("Client"+i+": Start");
	 }
	 
	 for(int i = 0; i < cCount; i++) {
		 try {
			clients[i].join();
		} catch (InterruptedException e) {
			System.out.println("Client Thread fail");
		}
		 System.out.println("Client"+i+": End");
	 }
	 
	 System.out.println("\n###END###\n");
 }
}
