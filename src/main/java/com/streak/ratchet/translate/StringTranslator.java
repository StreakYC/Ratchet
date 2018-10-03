package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key.Builder;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.AbstractMetadataField;

public class StringTranslator extends SimpleTranslator<String> {

	public StringTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
	}

	@Override public String translateSpannerValueToFieldType(Object item, Type innerType) {
		return String.valueOf(item);
	}

	@Override public void addValueToMutation(WriteBuilder builder,
											 String name,
											 String value) {
		if (null == value) {
			return;
		}
		builder.set(name).to(value);
	}

	@Override public void addValueToStatement(Statement.Builder builder,
											  String name,
											  int index, String value) {
		if (null == value) {
			return;
		}
		builder.bind(name + "_" + index).to(value);
	}

	@Override public void addToKeyBuilder(Builder builder, String value) {
		if (null == value) {
			return;
		}
		builder.append(value);
	}

	@Override public String translateSpannerValueToFieldType(Struct struct) {
		return struct.getString(metadataField.getName());
	}

	@Override public String spannerType() {
		return "STRING(MAX)";
	}
}
