package com.streak.ratchet;


import com.google.common.collect.Maps;

import java.util.Map;

public class Registrar {
	private Map<Class<?>, Metadata> metadata = Maps.newHashMap();

	public void register(Class<?> clazz) {
		try {
			metadata.put(clazz, new Metadata(clazz));
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> Metadata getMetadata(Class<T> type) {
		if (!isValid(type)) {
			throw new IllegalArgumentException("Trying to get metadata for unknown ratchet class: " + type);
		}

		return metadata.get(type);
	}

	public boolean isValid(Class<?> type) {
		return metadata.containsKey(type) && metadata.get(type).isValid();
	}
}
