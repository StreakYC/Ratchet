package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.streak.ratchet.AbstractMetadataField;
import com.streak.ratchet.Metadata;
import com.streak.ratchet.schema.ChildTable;
import com.streak.ratchet.schema.SpannerField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.streak.ratchet.Utils.getRaw;

public class ListTranslator implements Translator<List<?>> {
	private Metadata innerMetadata;
	private AbstractMetadataField metadataField;
	private String offsetFieldName;

	public ListTranslator(AbstractMetadataField metadataField) {
		this.metadataField = metadataField;
	}

	private String getOffsetFieldName() {
		if (null == offsetFieldName) {
			offsetFieldName = metadataField.asPrefix() + "offset";
			Boolean goodName;
			while (true) {
				goodName = true;
				for (SpannerField spannerField : this.metadataField.getTable().getSpannerFields()) {
					if (spannerField.getName().equals(offsetFieldName)) {
						goodName = false;
						break;
					}
				}
				if (goodName) {
					break;
				}
				offsetFieldName += "_offset";
			}
		}
		return offsetFieldName;
	}

	private Metadata getInnerMetadata() {
		if (null == innerMetadata) {
			List<SpannerField> additionalFields = new ArrayList<>();
			additionalFields.add(
				new SpannerField(metadataField.getTable(),
					getOffsetFieldName(),
					"INT64",
					true,
					false,
					false,
					true));
			this.innerMetadata = new Metadata(metadataField, additionalFields);
		}
		return innerMetadata;
	}

	private Stream<AbstractMetadataField> getAllStructuralFields(Metadata metadata) {
		Stream<AbstractMetadataField> fieldStream = metadata.getMetadataFields()
															.stream()
															.filter(f -> f.isKey || f.isVersion);
		if (metadata.parentMetadata == null) {
			return fieldStream;
		}
		else {
			return Stream.concat(fieldStream, getAllStructuralFields(metadata.parentMetadata));
		}
	}

	private Translator<Object> getInnerTranslator() {
		return getInnerMetadata().getMetadataField(metadataField.getName())
								 .getTranslator();
	}

	@Override public List<?> translateSpannerValueToFieldType(Object item, Type innerType) {
	    List ret = new ArrayList();
		ret.add(getInnerMetadata().getMetadataField(metadataField.getName())
											  .getTranslator().translateSpannerValueToFieldType(item, innerType));
		return ret;
	}

	@Override public void addValueToStatement(Builder builder, String name, int index, List<?> value) {
	}

	@Override public void addToKeyBuilder(Key.Builder builder, List<?> value) {
		throw new RuntimeException("Not Implemented");
	}

	@Override public List<?> translateSpannerValueToFieldType(Struct struct) {
		if (struct.getColumnType(metadataField.getName()).getCode() != Code.ARRAY) {
			throw new RuntimeException("Unable to deserialize value from spanner " +
				metadataField.getName() +
				" " +
				struct);
		}
		List<Struct> asList = struct.getStructList(metadataField.getName());

		if (!getInnerTranslator().getClass().equals(ListTranslator.class)) {
			return asList.stream().map(
				item -> {
					try {
						return getInnerTranslator().translateSpannerValueToFieldType(
							getRaw(item, item.getColumnIndex(metadataField.getName())),
							item.getType().getStructFields().get(item.getColumnIndex(metadataField.getName())).getType()
						);
					} catch (IllegalArgumentException ignore) {
						// TODO Be more consistent about responsibility to avoid things like this.
						return getInnerTranslator().translateSpannerValueToFieldType(item, item.getType());
					}
				}
			).collect(Collectors.toList());
		}

		return asList.stream().map(
			item -> {
				try {
					return getInnerMetadata().instanceFactory().constructFromStruct(item);
				}
				catch (Throwable throwable) {
					throw new RuntimeException(throwable);
				}
			}
		).collect(Collectors.toList());
	}

	@Override public List<ChildTable> asChildTables() {
		return Collections.singletonList((ChildTable) getInnerMetadata().getTable());
	}

	@Override public Collection<? extends Mutation> childMutations(Object instance) {
		try {
			if (null == metadataField.get(instance)) {
				return Collections.emptyList();
			}
			return childMutationsForValue(instance, metadataField.get(instance)).stream()
																				.map(WriteBuilder::build)
																				.collect(Collectors.toList());
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}

	}

	@Override public Collection<? extends WriteBuilder> childMutationsForValue(Object instance, List<?> value) {
		List<WriteBuilder> ret = new ArrayList<>();
		try {
			int i = 0;
			for (Object item : value) {
				WriteBuilder builder = Mutation.newInsertBuilder(getInnerMetadata().getTable().getName());
				getAllStructuralFields(metadataField.getMetadata()).forEach(f -> {
					try {
						f.addToMutationBuilder(instance, builder);
					}
					catch (Throwable throwable) {
						throw new RuntimeException(throwable);
					}
				});
				builder.set(getOffsetFieldName()).to(i);

				for (AbstractMetadataField field : getInnerMetadata().getMetadataFields()) {
					field.addToMutationBuilder(item, builder);
				}

				ret.add(builder);
				for (AbstractMetadataField field : getInnerMetadata().getMetadataFields()) {
					for (WriteBuilder childBuilder : field.getTranslator().childMutationsForValue(instance, item)) {
						childBuilder.set(getOffsetFieldName()).to(i);
						ret.add(childBuilder);
					}
				}
				i++;
			}
			return ret;
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}

	}
}
