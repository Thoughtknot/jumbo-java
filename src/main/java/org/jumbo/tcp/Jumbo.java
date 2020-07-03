package org.jumbo.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.jumbo.codec.Codec;
import org.jumbo.codec.CodecRegistry;
import org.jumbo.tcp.JumboTcpClient.Operation;

public class Jumbo {
	private final OutputStream outputStream;
	private final InputStream inputStream;
	private final CodecRegistry registry;

	Jumbo(Socket socket) throws IOException {
		outputStream = socket.getOutputStream();
		inputStream = socket.getInputStream();
		registry = CodecRegistry.get();
	}

	public void putObject(int table, Object key, Object value) throws IOException {
		Codec<Object, Object> val = registry.getCodec(table);
		if (val == null) {
			throw new RuntimeException("Could not find a codec registered for table " + table);
		}
		put(table, val.serializeKey(key), val.serializeValue(value));
	}
	
	public Object getObject(int table, Object key) throws IOException {
		Codec<Object, Object> val = registry.getCodec(table);
		if (val == null) {
			throw new RuntimeException("Could not find a codec registered for table " + table);
		}
		byte[] result = get(table, val.serializeKey(key));
		if (result.length == 0)
			return null;
		return val.deserializeValue(result);
	}

	public void deleteObject(int table, Object key) throws IOException {
		Codec<Object, Object> val = registry.getCodec(table);
		if (val == null) {
			throw new RuntimeException("Could not find a codec registered for table " + table);
		}
		delete(table, val.serializeKey(key));
	}
	
	public void put(int table, byte[] key, byte[] value) throws IOException {
		outputStream.write(Operation.PUT.getVal());
		outputStream.write(BytesUtil.intAsLe(table));
		outputStream.write(BytesUtil.intAsLe(key.length));
		outputStream.write(key);
		outputStream.write(BytesUtil.intAsLe(value.length));
		outputStream.write(value);
	}
	
	public byte[] get(int table, byte[] key) throws IOException {
		outputStream.write(Operation.GET.getVal());
		outputStream.write(BytesUtil.intAsLe(table));
		outputStream.write(BytesUtil.intAsLe(key.length));
		outputStream.write(key);
		
		byte[] lenBytes = new byte[4];
		inputStream.read(lenBytes);
		int len = BytesUtil.getLeInt(lenBytes);
		if (len == -1) {
			return new byte[0];
		}
		else {
			byte[] valueBytes = new byte[len];
			inputStream.read(valueBytes);
			return valueBytes;
		}
	}
	
	public void delete(int table, byte[] key) throws IOException {
		outputStream.write(Operation.DEL.getVal());
		outputStream.write(BytesUtil.intAsLe(table));
		outputStream.write(BytesUtil.intAsLe(key.length));
		outputStream.write(key);
	}
}