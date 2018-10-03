package com.streak.ratchet;

import com.google.cloud.spanner.TransactionContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class Writer {
	@Nullable
	private final TransactionContext transactionContext;
	final private Map<Class<?>, List<MutationModel>> instances = Maps.newHashMap();

	public Writer() {
		this(null);
	}

	public Writer(@Nullable TransactionContext transactionContext) {
		this.transactionContext = transactionContext;
	}

	public void addUpdate(Object instance) {
		add(MutationModel.forUpdate(instance));
	}

	public void add(MutationModel model) {
		Class<?> type = model.instance.getClass();

		if (!Configuration.INSTANCE.isValid(type)) {
			return;
		}

		if (instances.containsKey(type)) {
			instances.get(type).add(model);
		}
		else {
			instances.put(type, Lists.newArrayList(model));
		}
	}

	public void addDelete(Object instance) {
		add(MutationModel.forDelete(instance));
	}

	public void write() {
		if (instances.isEmpty()) {
			return;
		}
		ImmutableTransactionCallable callable = new ImmutableTransactionCallable(instances);
		if (transactionContext == null) {
			Configuration.INSTANCE
				.getSpannerClient()
				.readWriteTransaction()
				.run(callable);
		}
		else {
			callable.run(transactionContext);
		}
	}
}
