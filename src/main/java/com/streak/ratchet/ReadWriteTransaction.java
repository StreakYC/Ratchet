package com.streak.ratchet;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.TransactionRunner;

import javax.annotation.concurrent.Immutable;
import java.util.function.Function;

@Immutable
public class ReadWriteTransaction<R> {
	private R result;
	private final TransactionRunner txnRunner;
	private final Function<RatchetReaderWriter, R> work;

	public ReadWriteTransaction(Function<RatchetReaderWriter, R> work) {
		txnRunner = Configuration.INSTANCE.getSpannerClient().readWriteTransaction();
		this.work = work;
	}

	public void run() {
		result = txnRunner.run(txn -> work.apply(new RatchetReaderWriter(txn)));
	}

	public Timestamp getCommitTimestamp() {
		return txnRunner.getCommitTimestamp();
	}

	public R getResult() {
		return result;
	}
}
