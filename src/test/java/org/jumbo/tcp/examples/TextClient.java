package org.jumbo.tcp.examples;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.jumbo.tcp.JumboTcp;
import org.jumbo.tcp.JumboTcpClient;
import org.jumbo.tcp.JumboTcpClient.Operation;

public class TextClient {
	public static void main(String[] args) throws UnknownHostException, IOException {
		System.out.println("Started");
		JumboTcpClient client = JumboTcpClient.connect(Inet4Address.getLocalHost(), 8080);
		JumboTcp sender = client.blockUntilConnected();
		System.out.println("Usage: PUT/GET/DEL/KEYS <TABLE> <KEY/LIMIT> [<VALUE>]");
		Scanner scan = new Scanner(System.in);
		while (true) {
			String[] val = scan.nextLine().split(" ");
			try {
				Operation op = Operation.valueOf(val[0]);
				int table = Integer.valueOf(val[1]);
				String key = val[2];
				switch (op) {
				case DEL:
					sender.delete(table, key.getBytes());
					break;
				case GET:
					byte[] ret = sender.get(table, key.getBytes());
					System.out.println("GET: " + key + "," + new String(ret));
					break;
				case PUT:
					String value = String.join(" ", Arrays.copyOfRange(val, 3, val.length));
					sender.put(table, key.getBytes(), value.getBytes());
					break;
				case KEYS:
					List<byte[]> keys = sender.getKeys(table, Integer.valueOf(key));
					System.out.print("Keys: [");
					for (byte[] k : keys) {
						if (k != keys.get(0)) {
							System.out.print(", ");
						}
						System.out.print(new String(k));
					}
					System.out.print("]\n");
					break;
				default:
					break;
				
				}
			}
			catch (Exception e) {
				System.out.println("Error: " + e.getMessage());
			}
		}
	}
}
