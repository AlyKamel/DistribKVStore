package de.tum.i13.shared;

import java.nio.charset.Charset;

public class Constants {
	public static final String TELNET_ENCODING = "ISO-8859-1"; // encoding for telnet
	
	public static boolean canEncode(String input) {
	  return Charset.forName(TELNET_ENCODING).newEncoder().canEncode(input);
	}

}
