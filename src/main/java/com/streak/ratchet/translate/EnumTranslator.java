package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.AbstractMetadataField;

import static com.streak.ratchet.Utils.erase;

public class EnumTranslator<E extends Enum> extends SimpleTranslator<E> {
	private final Class<E> enumClass;

	public EnumTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
		this.enumClass = (Class<E>) erase(metadataField.getType());
	}

	@Override public E translateSpannerValueToFieldType(Object item, Type innerType) {
		return (E) Enum.valueOf(enumClass, (String) item);
	}

	@Override public void addValueToMutation(WriteBuilder builder, String name, E value) {
		if (null == value) {
			return;
		}
		builder.set(name).to(value.name());
	}

	@Override public void addValueToStatement(Builder builder, String name, int index, E value) {
		if (null == value) {
			return;
		}
		builder.bind(name + "_" + index).to(value.name());
	}

	@Override public void addToKeyBuilder(Key.Builder builder, E value) {
		if (null == value) {
			return;
		}
		builder.append(value.name());
	}

	@Override public E translateSpannerValueToFieldType(Struct struct) {
		return (struct.isNull(metadataField.getName())) ? null : (E) Enum.valueOf(enumClass, struct.getString(metadataField.getName()));
	}

	@Override public String spannerType() {
		return "STRING(MAX)";
	}
}
