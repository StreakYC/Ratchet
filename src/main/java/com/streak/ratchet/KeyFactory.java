package com.streak.ratchet;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.KeySet.Builder;

import java.util.Collection;

// For your keys and key related products
public class KeyFactory {
	private final Object instance;
	private final Metadata metadata;

	public KeyFactory(Object instance) {
		this.instance = instance;
		this.metadata = Configuration.INSTANCE.getMetadata(instance.getClass());
	}

	public static KeySet toTipConstraintKeys(Collection<MutationModel> models) throws Throwable {
		Builder builder = KeySet.newBuilder();
		for (MutationModel model : models) {
			builder.addKey(new KeyFactory(model.instance).toTipConstraintKey());
		}
		return builder.build();
	}

	public Key toTipConstraintKey() throws Throwable {
		Key.Builder keyBuilder = Key.newBuilder();
		for (AbstractMetadataField field : metadata.getKeyFields()) {
			field.getTranslator().addToKeyBuilder(keyBuilder, metadata.get(field.getName(), instance));
		}
		// tip field
		keyBuilder.append(true);
		return keyBuilder.build();
	}

	public KeyRange toKeyRange() throws Throwable {
		Key.Builder startBuilder = Key.newBuilder();
		Key.Builder endBuilder = Key.newBuilder();

		for (AbstractMetadataField field : metadata.getKeyFields()) {
			field.getTranslator().addToKeyBuilder(startBuilder, metadata.get(field.getName(), instance));
			field.getTranslator().addToKeyBuilder(endBuilder, metadata.get(field.getName(), instance));
		}
		startBuilder.append(Long.MIN_VALUE);
		endBuilder.append(Long.MAX_VALUE);

		return KeyRange.newBuilder().setStart(
			startBuilder.build()
		).setEnd(
			endBuilder.build()
		).build();
	}
}
