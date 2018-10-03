package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.AbstractMetadataField;

public class LongTranslator extends SimpleTranslator<Long> {

	public LongTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
	}

	@Override public Long translateSpannerValueToFieldType(Object item, Type innerType) {
		return (Long) item;
	}

	@Override public void addValueToMutation(WriteBuilder builder,
											 String name,
											 Long value) {
		if (null == value) {
			return;
		}
		builder.set(name).to(value);
	}

	@Override public void addValueToStatement(Builder builder, String name, int index, Long value) {
		if (null == value) {
			return;
		}
		builder.bind(name + "_" + index).to(value);
	}

	@Override public void addToKeyBuilder(Key.Builder builder, Long value) {
		if (null == value) {
			return;
		}
		builder.append(value);
	}

	@Override public Long translateSpannerValueToFieldType(Struct struct) {
		return struct.getLong(metadataField.getName());
	}

	@Override public String spannerType() {
		return "INT64";
	}
}
