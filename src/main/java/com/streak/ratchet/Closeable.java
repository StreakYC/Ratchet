package com.streak.ratchet;

public interface Closeable extends java.io.Closeable {
	// This is only extended so that we don't have to deal with the IOException
	@Override void close();
}

