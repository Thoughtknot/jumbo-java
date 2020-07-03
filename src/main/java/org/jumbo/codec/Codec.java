package org.jumbo.codec;

public interface Codec<K, V> {
	int getTable();
	byte[] serializeValue(V o);
	byte[] serializeKey(K o);
	V deserializeValue(byte[] b);
	K deserializeKey(byte[] b);
}
