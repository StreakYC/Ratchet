package com.streak.ratchet;

import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.streak.ratchet.schema.SpannerField;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import static com.streak.ratchet.Utils.innerType;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isStatic;

public class InnerMetadataField extends AbstractMetadataField {
	private final AbstractMetadataField parent;

	public InnerMetadataField(Metadata metadata, AbstractMetadataField parent) {
		super(metadata);
		this.parent = parent;
		type = innerType(parent.getType());
		name = parent.getName();
		isKey = false;
		isTip = false;
		isVersion = false;
		notNull = false;

		translator = Configuration.INSTANCE.getTranslator(this);
	}

	@Override protected List<SpannerField> makeSpannerFields() {
		return translator.asSpannerFields().stream()
						 .peek(field -> field.setPrefix(parent.asPrefix())) // ew ew ew (still)
						 .collect(Collectors.toList());
	}

	@Override protected String spannerFieldName() {
		return parent.asPrefix() + getName();
	}

	@Override public void set(Object instance, Object value) throws Throwable {
		/*
		TODO
		The fields are a much better place for complexity, but since this was added late in the development, we are left
		with some really awkward corners, such as this one.
		*/
		for (Field field : instance.getClass().getDeclaredFields()) {
			if (isStatic(field.getModifiers())) {
				continue;
			}
			if (isFinal(field.getModifiers())) {
				continue;
			}
			field.setAccessible(true);
			field.set(instance, field.get(value));
		}
	}

	public void addToMutationBuilder(Object instance, WriteBuilder builder) throws Throwable {
		translator.addValueToMutation(builder, parent.asPrefix() + name, get(instance));
	}

	@Override public <E> E get(Object instance) throws Throwable {
		return (E) instance;
	}

	@Override public String asPrefix() {
		return this.parent.asPrefix();
	}
}
