package com.streak.ratchet;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.streak.ratchet.Utils.getRaw;

// - this is an immutable specific writer - are we cool with that restriction?
//   for now, it makes things way easier.
public class ImmutableTransactionCallable implements TransactionCallable<Void> {
	private final Map<Class<?>, List<MutationModel>> instances;

	public ImmutableTransactionCallable(Map<Class<?>, List<MutationModel>> instances) {
		this.instances = instances;
	}

	@Nullable @Override public Void run(TransactionContext context) {
		try {
			final List<Mutation> mutations = new ArrayList<>();
			Map<ComparableKey, Long> tips = getTips(context);
			for (Entry<Class<?>, List<MutationModel>> entry : instances.entrySet()) {
				Metadata metadata = Configuration.INSTANCE.getMetadata(entry.getKey());
				for (MutationModel model : entry.getValue()) {
					addMutationsForInstance(metadata, model.instance, model.isDelete, mutations, tips);
				}
			}
			// We buffer at the end because we want the tip removals before the insertions
			context.buffer(mutations);
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
		return null;
	}

	/**
	 * Query spanner for the tip versions for each key in our transaction
	 * <p>
	 * We can use the key since all key data types are hashable/comparable. This isn't the most obvious place for this function
	 *
	 * @param context The ReadContext of the transaction
	 * @return A map of Key to the tip versions (there should *always* be only one)
	 * @throws Throwable
	 */
	private Map<ComparableKey, Long> getTips(ReadContext context) throws Throwable {
		Map<ComparableKey, Long> tips = new HashMap<>();
		for (Class<?> type : instances.keySet()) {
			Metadata metadata = Configuration.INSTANCE.getMetadata(type);
			String versionFieldName = metadata.getVersionFieldName();
			int numberKeyColumns = metadata.getTable().primaryFields().size();
			try (ResultSet rs = getTips(type, context)) {
				while (rs.next()) {
					Struct struct = rs.getCurrentRowAsStruct();
					List<Object> keyParts = new ArrayList<>();
					int versionFieldIndex = struct.getType().getFieldIndex(versionFieldName);
					Long version = 0L;
					int index = 0;
					while (index < numberKeyColumns) {
						if (index == versionFieldIndex) {
							version = rs.getLong(index);
						}
						else {
							keyParts.add(getRaw(struct, index));
						}
						index += 1;
					}
					ComparableKey key = new ComparableKey(keyParts);
					if (!tips.containsKey(key)) {
						tips.put(key, version);
					}
					else if (!tips.get(key).equals(version)) {
						throw new RuntimeException("Got two versions (" +
							tips.get(key) +
							" and " +
							version +
							") for " +
							keyParts);
					}
				}
			}
		}
		return tips;
	}

	private void addMutationsForInstance(Metadata metadata,
										 Object instance,
										 boolean isDelete,
										 List<Mutation> mutations,
										 Map<ComparableKey, Long> tips) throws Throwable {

		ComparableKey key = comparableKey(metadata, instance);
		MutationFactory mutationFactory = new MutationFactory(instance);

		Long version;
		if (tips.containsKey(key)) {
			Long tipVersion = tips.get(key);
			mutations.add(0,
				mutationFactory.removeTipMutation(tipVersion));
			version = 1L + tipVersion;
		}
		else {
			version = 1L;
		}
		metadata.setVersion(instance, version);
		metadata.setTip(instance, isDelete ? null : true);

		if (!isDelete) {
			mutations.addAll(mutationFactory.createInsert());
		}
	}

	private ResultSet getTips(Class<?> type, ReadContext context) throws Throwable {
		Metadata metadata = Configuration.INSTANCE.getMetadata(type);
		KeySet keySet = KeyFactory.toTipConstraintKeys(instances.get(type));
		return context.readUsingIndex(
			metadata.getTableName(),
			metadata.tipConstraint(),
			keySet,
			metadata.tipConstraintColumns()
		);
	}

	private ComparableKey comparableKey(Metadata metadata, Object instance) throws Throwable {
		List<Object> keyParts = new ArrayList<>();
		for (AbstractMetadataField field : metadata.getKeyFields()) {
			keyParts.addAll(field.getTranslator().values(field.get(instance)));
		}
		return new ComparableKey(keyParts);
	}

	public static class ComparableKey implements Comparable<ComparableKey> {
		private final List<Object> keyParts;

		public ComparableKey(List<Object> keyParts) {
			this.keyParts = keyParts;
		}

		@Override
		public int hashCode() {
			return keyParts.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ComparableKey)) {
				return false;
			}

			return keyParts.equals(((ComparableKey) obj).keyParts);
		}

		@Override
		public int compareTo(@Nonnull ComparableKey o) {
			if (o.keyParts.size() != keyParts.size()) {
				return o.keyParts.size() - keyParts.size();
			}

			for (int i = 0; i < keyParts.size(); i++) {
				if (((Comparable) o.keyParts.get(i)).compareTo(keyParts.get(i)) != 0) {
					return ((Comparable) o.keyParts.get(i)).compareTo(keyParts.get(i));
				}
			}

			return 0;
		}
	}
}
