package com.streak.ratchet;

import com.google.cloud.spanner.TransactionContext;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class RatchetReaderWriter extends RatchetReader {
	private TransactionContext transactionContext;

	public RatchetReaderWriter() {
	}

	public RatchetReaderWriter(TransactionContext transactionContext) {

		this.transactionContext = transactionContext;
	}

	// Saves within a transaction are not visible until after the transaction is committed.
	public void save(Object instance) {
		try {
			Writer writer = new Writer(getTransactionContext());
			writer.addUpdate(instance);
			writer.write();
		}
		catch (RuntimeException | Error e) {
			throw e;
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	@Nullable
	protected TransactionContext getTransactionContext() {
		return transactionContext;
	}

	// Deletes within a transaction are not visible until after the transaction is committed.
	public void delete(Object instance) {
		try {
			Writer writer = new Writer(getTransactionContext());
			writer.addDelete(instance);
			writer.write();
		}
		catch (RuntimeException | Error e) {
			throw e;
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
}
