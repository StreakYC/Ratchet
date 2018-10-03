package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.AbstractMetadataField;

public class BooleanTranslator extends SimpleTranslator<Boolean> {

	public BooleanTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
	}

	@Override public Boolean translateSpannerValueToFieldType(Object item, Type innerType) {
		return (Boolean) item;
	}

	@Override public void addValueToMutation(WriteBuilder builder,
											 String name,
											 Boolean value) {
		if (null == value) {
			return;
		}
		builder.set(name).to(value);
	}

	@Override public void addValueToStatement(Builder builder, String name, int index, Boolean value) {
		if (null == value) {
			return;
		}
		builder.bind(name + "_" + index).to(value);
	}

	@Override public void addToKeyBuilder(Key.Builder builder, Boolean value) {
		if (null == value) {
			return;
		}
		builder.append(value);
	}

	@Override public Boolean translateSpannerValueToFieldType(Struct struct) {
		return (struct.isNull(metadataField.getName())) ? null : struct.getBoolean(metadataField.getName());
	}

	@Override public String spannerType() {
		return "BOOL";
	}
}
