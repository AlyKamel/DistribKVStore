package de.tum.i13;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import de.tum.i13.client.ClientLibrary;

public class ClientThread extends Thread {
	String pref; //prefix for prints on console
	String path; //directory path to the .txt for commands
	ClientLibrary cl = new ClientLibrary();
	String first;
	int mode;
	
	public ClientThread(String id, String addr, int port, String path) {
		super();
		pref = "Client "+id+": ";
		this.path = path;
		try {
			System.out.println(pref+"START");
			System.out.println(pref+"addr ="+addr+" port ="+port);
			System.out.println("CONN "+cl.connect(addr, port));
			System.out.println("USER "+cl.setUsername("Client"+id));
			
		} catch (IOException e) {
			System.out.println(pref+"Connection error");
		}
	}
	
	@Override
	public void run() {
		mode = 0; //=put
		System.out.println("\n"+pref+"DOING PUT");
		test();
		mode = 1; //=get
		System.out.println("\n"+pref+"DOING GET");
		test();
		mode = 2; //=del
		System.out.println("\n"+pref+"DOING DEL");
		test();
	}
	
	private void test() {
		try {
			File file = new File(path);
    	BufferedReader read = new BufferedReader(new FileReader(file));
    	System.out.println(pref+"Opened file");
    	String line = read.readLine();
    	while(line != null) {
    		System.out.println(pref+doLine(line));
    		//doLine2(line);
 	      line = read.readLine();
    	}
	    read.close();
    } catch (FileNotFoundException e) {
			System.out.println(pref+"file not found");
		} catch (IOException e) {
    	System.out.println(pref+"line read fail or doLine fail");
    }
	}
	
	private String doLine(String line) {
		String[] comp = line.split("\\s+",2);
		System.out.println("Comp "+comp[0]+" "+comp[1]);
		switch(mode) {
		case 0: 
			try {
				return cl.putRequest(comp[0], "\"" + comp[1]+ "\"");
			} catch (IOException e) {
				return "put fail";
			}
		case 1:
			try {
				return cl.getRequest(comp[0]);
			} catch (IOException e) {
				return "get fail";
			}
		case 2:
			try {
				return cl.deleteRequest(comp[0]);
			} catch (IOException e) {
				return "delete fail";
			}
		default:
			return "unknown command, not put, get or del";
		}
	}
	
	private void doLine2(String line) {
		String[] comp = line.split("\\s+",2);
		switch(mode) {
		case 0: 
			try {
				assertEquals("SUCCES",cl.putRequest(comp[0], "\"" + comp[1]+ "\""));
				break;
			} catch (IOException e) {
				System.out.println("put fail");
				break;
			}
		case 1:
			try {
				assertEquals(comp[1], cl.getRequest(comp[0]));
				break;
			} catch (IOException e) {
				System.out.println("get fail");
				break;
			}
		case 2:
			try {
				assertEquals("SUCCESS", cl.deleteRequest(comp[0]));
			} catch (IOException e) {
				System.out.println("delete fail");
				break;
			}
		default:
			System.out.println("unknown command, not put, get or del");
		}
	}
}
