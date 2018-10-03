package com.streak.ratchet;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Factory for producing instances classes that map to spanner entities
 * <p>
 * This used to do a lot more.  It should probably go away now.
 */
public class InstanceFactory {
	private final Metadata metadata;

	public InstanceFactory(Metadata metadata) {
		this.metadata = metadata;
	}

	public <T> T constructFromStruct(Struct struct) throws Throwable {
		T instance = metadata.newInstance();
		fill(struct, instance);
		return instance;
	}

	private <T> void fill(Struct struct, T instance) throws Throwable {
		for (AbstractMetadataField field : metadata.getMetadataFields()) {
			field.fillInstanceFromStruct(instance, struct);
		}
	}

	public <T> List<T> constructFromResultSet(ResultSet resultSet) throws Throwable {
		List<T> ret = Lists.newArrayList();
		while (resultSet.next()) {
			T instance = metadata.newInstance();
			fill(resultSet.getCurrentRowAsStruct(), instance);
			ret.add(instance);
		}
		return ret;
	}
}
