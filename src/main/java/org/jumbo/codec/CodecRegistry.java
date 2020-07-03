package org.jumbo.codec;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CodecRegistry {
	private Map<Integer, Codec<?, ?>> codecsByTable = new ConcurrentHashMap<>();
	
	private static CodecRegistry instance = null;
	
	private CodecRegistry() {
		//
	}
	
	public void registerCodec(int table, Codec<?, ?> codec) {
		codecsByTable.put(table, codec);
	}
	
	@SuppressWarnings("unchecked")
	public <K, V> Codec<K, V> getCodec(int table) {
		return (Codec<K, V>) codecsByTable.get(table);
	}
	
	public static synchronized CodecRegistry get() {
		if (instance == null) {
			instance = new CodecRegistry();
		}
		return instance;
	}

}
