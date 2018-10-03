package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.AbstractMetadataField;
import com.streak.ratchet.NestedMetadataField;
import com.streak.ratchet.Utils;
import com.streak.ratchet.schema.ChildTable;
import com.streak.ratchet.schema.SpannerField;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.streak.ratchet.Utils.erase;

/** Translator for arbitrary classes.
 *
 */
public class DataClassTranslator implements Translator<Object> {
	private AbstractMetadataField metadataField;
	private Map<String, AbstractMetadataField> metadataFieldMap;

	public DataClassTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
	}

	private Collection<AbstractMetadataField> getMetadataFields() {
		if (null == metadataFieldMap) {
			metadataFieldMap = new HashMap<>();
			Map<String, Field> fieldsByName = Utils.createFieldsByName(erase(metadataField.getType()));
			for (Entry<String, Field> entry : fieldsByName.entrySet()) {
				try {
					metadataFieldMap.put(entry.getKey(), new NestedMetadataField(metadataField, entry.getValue()));
				}
				catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			}
			// TODO - we aren't actually using calculated fields anywhere, but this should be fleshed out.
			//for (Entry<String, Method> entry : Utils.getCalculatedFields(entityClass).entrySet()) {
			//	metadataFieldMap.put(entry.getKey(), new MetadataCalculatedField(entry.getValue(), this));
			//}
		}
		return metadataFieldMap.values();
	}

	@Override public void addValueToMutation(WriteBuilder builder, String name, Object instance) {
		for (AbstractMetadataField field : getMetadataFields()) {
			try {
				field.getTranslator().addValueToMutation(builder, metadataField.asPrefix() + field.getName(), field.get(instance));
			}
			catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}
		}
	}

	@Override public void addValueToStatement(Builder builder, String name, int index, Object instance) {
		for (AbstractMetadataField field : getMetadataFields()) {
			try {
				field.getTranslator()
					 .addValueToStatement(builder, name + "_" + field.getName(), index, field.get(instance));
			}
			catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}
		}
	}

	@Override public void addToKeyBuilder(Key.Builder builder, Object value) {
		throw new RuntimeException("Not yet supported");
	}

	@Override public List<SpannerField> asSpannerFields() {
		return getMetadataFields()
							.stream()
							.flatMap(field -> field.getSpannerFields().stream())
							.collect(Collectors.toList());
	}

	@Override public Object translateSpannerValueToFieldType(Struct struct) {
		List<Struct> innerStructList = struct.getStructList(metadataField.getName());
		return translateSpannerValueToFieldType(innerStructList, struct.getColumnType(metadataField.getName()));
	}

	private Object constructFromStruct(Struct struct) {
		try {
			Object instance = erase(metadataField.getType()).newInstance();
			for (AbstractMetadataField field : getMetadataFields()) {
				field.fillInstanceFromStruct(instance, struct);
			}
			return instance;
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
	}

	@Override public Object translateSpannerValueToFieldType(Object item, Type innerType) {
		try {
			switch (innerType.getCode()) {
				case BOOL:
				case INT64:
				case FLOAT64:
				case STRING:
				case BYTES:
				case TIMESTAMP:
				case DATE:
					throw new RuntimeException("I have no idea why I am seeing: " + item);
				case ARRAY:
					//noinspection unchecked
					List<Struct> items = (List<Struct>) item;
					List<Object> ret = new ArrayList<>();
					for (Struct struct : items) {
						ret.add(constructFromStruct(struct));
					}
					if (ret.size() != 1) {
						throw new RuntimeException("Expecting scalar");
					}
					return ret.get(0);
				case STRUCT:
					return constructFromStruct((Struct) item);
			}
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
		return null;
	}

	@Override public List<ChildTable> asChildTables() {
		return getMetadataFields()
							.stream()
							.flatMap(field -> field.getTranslator().asChildTables().stream())
							.collect(Collectors.toList());
	}

	@Override public Collection<? extends Mutation> childMutations(Object instance) {
		return getMetadataFields()
							.stream()
							.flatMap(field -> field.getTranslator().childMutations(instance).stream())
							.collect(Collectors.toList());
	}

	@Override public Collection<? extends WriteBuilder> childMutationsForValue(Object instance, Object value) {
		return getMetadataFields()
							.stream()
							.flatMap(field -> {
								try {
									return field.getTranslator()
														   .childMutationsForValue(instance, field.get(value))
														   .stream();
								}
								catch (Throwable throwable) {
									throw new RuntimeException(throwable);
								}
							})
							.collect(Collectors.toList());
	}
}
