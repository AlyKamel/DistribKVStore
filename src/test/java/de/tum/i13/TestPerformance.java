package de.tum.i13;

import static de.tum.i13.Util.createECSServer;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import de.tum.i13.client.ClientApp;
import de.tum.i13.server.ecs.HashRing;
import de.tum.i13.server.ecs.MainECS;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.ServerUtility;

public class TestPerformance {

	private static Thread server;
	private static Thread ecs;
	private static String address;
	private static int serverport;
	private static PrintWriter output;
	private static BufferedReader input;
	private static Socket socket;
	private static String welcome;
	private static int ecsport;
	private static String bootstrap;
	private static HashRing hr;

	@BeforeAll
	public static void beforeAll() throws IOException, InterruptedException {
		address = "127.0.0.1";
		ecsport = ServerUtility.getFreePort(address);

		Logger.getLogger(MainECS.class.getName()).setLevel(Level.ALL);
		Logger.getLogger(Main.class.getName()).setLevel(Level.ALL);
		Logger.getLogger(ClientApp.class.getName()).setLevel(Level.ALL);

		ecs = createECSServer(address, ecsport); // ecs server is created
		ecs.start();
	}
	
	/**
	 * creates a server with given caching strategy with the help of the
	 * createServerWithCachingStrategy(cachingStrategy) method 
	 * KVStore, HashRing and ServerRing are created 
	 * @param cachingStrategy
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void createServer(String cachingStrategy) throws IOException, InterruptedException {

		serverport = ServerUtility.getFreePort(address);
		bootstrap = ServerUtility.getSocketAddress(address, ecsport);
		server = createServerWithCachingStrategy(cachingStrategy);
		server.start();
		Thread.sleep(1000);

		hr = new HashRing();
		String serverAddress = address + ":" + serverport;
		hr.addServer(serverAddress);
		hr.updateBuf();
	}

	/**
	 * creates servers with different variables such as: number of servers and
	 * caching strategy
	 * 
	 * @param servernum
	 * @param cachingStrategy
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void createServers(int serverNum, String cachingStrategy) throws IOException, InterruptedException {
		System.out.println("Creating " + serverNum + " servers");
		for(int i = 1; i <= serverNum; i++) {
			createServer(cachingStrategy);
			System.out.println("Server" + i + " is created");
		}
	}
	
	/**
	 * creates servers with "LRU", "LFU" or "FIFO"
	 * 
	 * @param cachingStrategy
	 * @return
	 */
	public Thread createServerWithCachingStrategy(String cachingStrategy) {
		switch (cachingStrategy) {
		case "LRU":
			server = Util.createServerWithCachingStrategy(address, serverport, bootstrap, "LRU");
			return server;
		case "LFU":
			server = Util.createServerWithCachingStrategy(address, serverport, bootstrap, "LFU");
			return server;
		case "FIFO":
			server = Util.createServerWithCachingStrategy(address, serverport, bootstrap, "FIFO");
			return server;
		default:
			System.out.println("Server cannot be created!!!!");
			break;
		}
		return null;
	}
	
	/**
	 * creates a client
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void createClient() throws UnknownHostException, IOException {
		socket = new Socket(address, serverport);
		output = new PrintWriter(socket.getOutputStream());
		input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		welcome = input.readLine();
	}

	/**
	 * creates clients with given number of clients
	 * @param clientnum
	 * @throws IOException
	 */
	public void createClients(int clientNum) throws IOException {
		System.out.println("Creating " + clientNum + " clients");
		for(int i = 1; i <= clientNum; i++) {
			createClient();
			System.out.println("Client " + i + " is ready");
		}
	}

	@AfterEach
	public void afterEach() throws IOException {
		socket.close();
		input.close();
		output.close();
	}

	@Test
	public void estimatedTimeTest() throws IOException, InterruptedException {

		createServers(1, "LRU");
		//createServers(5, "LRU");
		// createServers(10, "LRU");

		//createServers(1, "LFU");
		// createServers(5, "LFU");
		// createServers(10, "LFU");

		//createServers(1, "FIFO");
		//createServers(5, "FIFO");
		// createServers(10, "FIFO");

		createClients(1);
		// createClients(5);
		// createClients(10);

		// tests put request

		long startTime1 = System.currentTimeMillis(); // we want to see how long does it take a put request
		String command1 = "put key value";
		output.println(command1);
		output.flush();

		String res1 = input.readLine();
		System.out.println(res1); // testing
		
		if(res1.equals("server_not_responsible")) {
		String server = hr.getCoordinator("key");
		String[] address = server.split(":");
		InetSocketAddress socketAddress = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
			socket.bind(socketAddress);
			
			command1 = "put key value";
			output.println(command1);
			output.flush();

			res1 = input.readLine();
			System.out.println(res1); // testing
		}
		
		assertThat(res1, is(containsString("put_")));// testing

		long estimatedTime1 = System.currentTimeMillis() - startTime1;
		System.out.println("Put request took: " + estimatedTime1 + " milliseconds");

		// tests get request
		long startTime2 = System.currentTimeMillis(); // we want to see how long does it take a get request
		String command2 = "get key";
		output.println(command2);
		output.flush();

		String res2 = input.readLine();
		
		if(res1.equals("server_not_responsible")) {
			ArrayList<String> serverList = hr.getReadResponsibleServers("key");
			Random random = new Random();
			String server = serverList.get(random.nextInt(2 + 1));
			String[] address = server.split(":");
			InetSocketAddress socketAddress = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
				socket.bind(socketAddress);
				
				command1 = "put key value";
				output.println(command1);
				output.flush();

				res1 = input.readLine();
				System.out.println(res1); // testing
			}
		System.out.println(res2); // testing
		assertThat(res2, is(containsString("get_")));// testing
		long estimatedTime2 = System.currentTimeMillis() - startTime2;
		System.out.println("Get request took: " + estimatedTime2 + " milliseconds");

		// tests delete request
		long startTime3 = System.currentTimeMillis(); // we want to see how long does it take a delete request
		String command3 = "delete key";
		output.println(command3);
		output.flush();

		String res3 = input.readLine();
		System.out.println(res3); // testing
		

		if(res3.equals("server_not_responsible")) {
		String server = hr.getCoordinator("key");
		String[] address = server.split(":");
		
		InetSocketAddress socketAddress = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
			socket.bind(socketAddress);
			
			command3 = "put key value";
			output.println(command3);
			output.flush();

			res1 = input.readLine();
			System.out.println(res3); // testing
		}
		
		assertThat(res3, is(containsString("delete_")));// testing
		long estimatedTime3 = System.currentTimeMillis() - startTime3;
		System.out.println("Delete request took: " + estimatedTime3 + " milliseconds");
	}
}

