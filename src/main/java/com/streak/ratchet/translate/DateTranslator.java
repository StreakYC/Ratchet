package com.streak.ratchet.translate;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.AbstractMetadataField;

import java.util.Date;

public class DateTranslator extends SimpleTranslator<Date> {

	public DateTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
	}

	@Override public void addValueToMutation(WriteBuilder builder, String name, Date value) {
		if (null == value) {
			return;
		}
		builder.set(name).to(com.google.cloud.Timestamp.of(value));
	}

	@Override public void addValueToStatement(Builder builder, String name, int index, Date value) {
		if (null == value) {
			return;
		}
		builder.bind(name + "_" + index).to(com.google.cloud.Timestamp.of(value));
	}

	@Override public void addToKeyBuilder(Key.Builder builder, Date value) {
		if (null == value) {
			return;
		}
		builder.append(com.google.cloud.Timestamp.of(value));
	}

	@Override public Date translateSpannerValueToFieldType(Struct struct) {
		if (struct.isNull(metadataField.getName())) {
			return null;
		}
		Type type = struct.getColumnType(metadataField.getName());
		switch (type.getCode()) {
			case TIMESTAMP:
				return translateSpannerValueToFieldType(struct.getTimestamp(metadataField.getName()), type);
			case DATE:
				return translateSpannerValueToFieldType(struct.getDate(metadataField.getName()), type);
			default:
				throw new AssertionError("Invalid type" + type);
		}

	}

	@Override public Date translateSpannerValueToFieldType(Object item, Type type) {
		if (null == item) {
			return null;
		}
		switch (type.getCode()) {
			case TIMESTAMP:
				Timestamp ts = (Timestamp) item;
				return new Date(ts.getSeconds() * 1000 + ts.getNanos() / 1000000);
			case DATE:
				com.google.cloud.Date dt = (com.google.cloud.Date) item;
				return new Date(
					dt.getYear(),
					dt.getMonth(),
					dt.getDayOfMonth()
				);
			default:
				throw new AssertionError("Invalid type" + type);
		}
	}

	@Override public String spannerType() {
		return "TIMESTAMP";
	}
}
