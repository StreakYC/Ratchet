package com.streak.ratchet;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.common.collect.Lists;

import java.util.List;

public class MutationFactory {

	private final Object instance;
	private final Metadata metadata;

	public MutationFactory(Object instance) {
		this.instance = instance;
		this.metadata = Configuration.INSTANCE.getMetadata(instance.getClass());
	}

	public List<Mutation> createInsert() throws Throwable {
		List<Mutation> ret = Lists.newArrayList();
		WriteBuilder builder = Mutation.newInsertBuilder(metadata.getTableName());
		populateMutationBuilder(builder);
		ret.add(builder.build());
		for (AbstractMetadataField field : metadata.getMetadataFields()) {
			ret.addAll(field.childInserts(instance));
		}
		return ret;
	}

	private void populateMutationBuilder(Mutation.WriteBuilder builder) throws Throwable {
		for (AbstractMetadataField field : metadata.getMetadataFields()) {
			field.addToMutationBuilder(instance, builder);
		}
	}

	public Mutation removeTipMutation(long versionId) throws Throwable {
		WriteBuilder builder = Mutation.newUpdateBuilder(metadata.getTableName());
		for (AbstractMetadataField field : metadata.getKeyFields()) {
			field.addToMutationBuilder(instance, builder);
		}
		builder.set(metadata.getIsTipFieldName()).to((Boolean) null);
		builder.set(metadata.getVersionFieldName()).to(versionId);
		return builder.build();
	}
}
