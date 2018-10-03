package com.streak.ratchet;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Struct;
import com.streak.ratchet.schema.ChildTable;
import com.streak.ratchet.schema.Selectable;
import com.streak.ratchet.schema.SpannerField;
import com.streak.ratchet.schema.SpannerTable;
import com.streak.ratchet.translate.Translator;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractMetadataField {
	protected static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = new HashMap<>();

	static {
		PRIMITIVE_TO_WRAPPER.put(boolean.class, Boolean.class);
		PRIMITIVE_TO_WRAPPER.put(byte.class, Byte.class);
		PRIMITIVE_TO_WRAPPER.put(short.class, Short.class);
		PRIMITIVE_TO_WRAPPER.put(int.class, Integer.class);
		PRIMITIVE_TO_WRAPPER.put(long.class, Long.class);
		PRIMITIVE_TO_WRAPPER.put(float.class, Float.class);
		PRIMITIVE_TO_WRAPPER.put(double.class, Double.class);
		PRIMITIVE_TO_WRAPPER.put(char.class, Character.class);
	}

	public Boolean isTip;
	public Boolean isVersion;
	public Boolean isKey;
	public Boolean notNull;
	protected Metadata metadata;
	protected List<SpannerField> spannerFields;
	protected String name;
	protected Translator<?> translator;
	protected MethodHandle setter;
	protected MethodHandle getter;
	protected Type type;
	protected List<ChildTable> childTables;

	public AbstractMetadataField(
		Metadata metadata) {
		this.metadata = metadata;

	}

	public Boolean getNotNull() {
		return notNull;
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public Type getType() {
		return type;
	}

	public <T> void fillInstanceFromStruct(T instance, Struct struct) throws Throwable {
		try { // I really hate the interface to Struct
			if (struct.isNull(getName())) {
				return;
			}
		}
		catch (IllegalArgumentException ignore) {
			return;
		}
		Object value = getTranslator().translateSpannerValueToFieldType(struct);
		set(instance, value);
	}

	public String getName() {
		return name;
	}

	public <E> Translator<E> getTranslator() {
		//noinspection unchecked
		return (Translator<E>) translator;
	}

	public void set(Object instance, Object value) throws Throwable {
		getSetter().invoke(instance, value);
	}

	public MethodHandle getSetter() {
		return setter;
	}

	public void addToMutationBuilder(Object instance, WriteBuilder builder) throws Throwable {
		translator.addValueToMutation(builder, name, get(instance));
	}

	public <E> E get(Object instance) throws Throwable {
		//noinspection unchecked
		return (E) getter.invoke(instance);
	}

	public String asSelect() {
		// TODO Move the wrapping decision into the translator itself.
		Stream<String> childSelect = getChildTables().stream().map(ChildTable::asSelect);
		if (getSpannerFields().size() == 1 && getSpannerFields().get(0).getName().equals(spannerFieldName())) {
			return getSpannerFields().get(0).asSelect();
		}
		else if (getSpannerFields().size() > 0) {
			return String.format(
				" ARRAY(\n\tSELECT AS STRUCT %s \n) AS %s",
				Stream.concat(getSpannerFields().stream().map(Selectable::asSelect), childSelect)
					   .collect(Collectors.joining(",\n\t")),
				getName());

		}
		else {
			return childSelect.collect(Collectors.joining(" , "));
		}
	}

	public List<ChildTable> getChildTables() {
		if (null == childTables) {
			childTables = translator.asChildTables();
		}
		return childTables;
	}

	public List<SpannerField> getSpannerFields() {
		if (null == spannerFields) {
			spannerFields = makeSpannerFields();
		}
		return spannerFields;
	}

	protected String spannerFieldName() {
		return getName();
	}

	public SpannerTable getTable() {
		return metadata.getTable();
	}

	protected abstract List<SpannerField> makeSpannerFields();

	public Collection<? extends Mutation> childInserts(Object instance) {
		return translator.childMutations(instance);
	}

	public String asPrefix() {
		return getName() + "_";
	}
}
