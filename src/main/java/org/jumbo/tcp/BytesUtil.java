package org.jumbo.tcp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BytesUtil {

	public static byte[] intAsLe(int i) {
		ByteBuffer keyInt = ByteBuffer.allocate(4);
		keyInt.order(ByteOrder.LITTLE_ENDIAN);
		keyInt.putInt(i);
		return keyInt.array();
	}

	public static int getLeInt(byte[] lenBytes) {
		return ByteBuffer.wrap(lenBytes)
			.order(ByteOrder.LITTLE_ENDIAN)
			.getInt();
	}
}
