package org.jumbo.tcp;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class JumboTcpClient implements Closeable {
	private final AtomicReference<Socket> socket;
	private final AtomicBoolean running;

	private JumboTcpClient(AtomicReference<Socket> socket, AtomicBoolean running) {
		this.socket = socket;
		this.running = running;
	}

	public enum Operation {
		PUT((byte) 11), GET((byte) 21), DEL((byte) 31);

		private byte val;

		Operation(byte val) {
			this.val = val;
		}

		public byte getVal() {
			return val;
		}
	}

	public Jumbo blockUntilConnected() throws IOException {
		while (socket.get() == null) {
			try {
				Thread.sleep(100);
			} 
			catch (InterruptedException e) {
				// Do nothing.
			}
		}
		return new Jumbo(socket.get());
	}
	
	public Optional<Jumbo> ifConnected() throws IOException {
		if (socket.get() != null) {
			return Optional.of(new Jumbo(socket.get()));
		}
		else {
			return Optional.empty();
		}
	}
	
	@Override
	public void close() throws IOException {
		running.set(false);
		Socket socket = this.socket.get();
		if (socket != null) {
			socket.close();
		}
	}
	
	public static JumboTcpClient connect(InetAddress address, int port) {
		final AtomicReference<Socket> socket = new AtomicReference<>();
		final AtomicBoolean running = new AtomicBoolean(true);
		Thread thread = new Thread(() -> {
			Socket clientSocket = null;
			while (running.get() && clientSocket == null) {
				try {
					clientSocket = new Socket(address, port);
					socket.set(clientSocket);
				}
				catch (Exception e) {
					continue;
				}
			}
		});
		thread.setDaemon(true);
		thread.setName("JumboConnector-" + address.toString());
		thread.start();
		return new JumboTcpClient(socket, running);
	}
}
