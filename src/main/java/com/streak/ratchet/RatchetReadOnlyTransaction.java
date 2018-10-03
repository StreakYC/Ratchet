package com.streak.ratchet;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;

import javax.annotation.concurrent.Immutable;

@Immutable
public class RatchetReadOnlyTransaction extends RatchetReader implements Closeable {
	private final ReadOnlyTransaction transaction;

	public RatchetReadOnlyTransaction() {
		transaction = Configuration.INSTANCE.getSpannerClient().readOnlyTransaction();
	}

	@Override
	protected ReadContext getReadContext() {
		return transaction;
	}

	public Timestamp getReadTimestamp() {
		return transaction.getReadTimestamp();
	}

	@Override
	public void close() {
		transaction.close();
	}
}
