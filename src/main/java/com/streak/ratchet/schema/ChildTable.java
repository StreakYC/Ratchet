package com.streak.ratchet.schema;

import com.streak.ratchet.AbstractMetadataField;
import com.streak.ratchet.Metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChildTable implements SpannerTable, Selectable {
	private final SpannerTable parent;
	private final Metadata metadata;
	private String name;
	private List<SpannerField> additionalFields;
	private AbstractMetadataField innerMetadataField;

	public ChildTable(Metadata metadata, String name,
					  SpannerTable parent,
					  List<SpannerField> additionalFields, AbstractMetadataField innerMetadataField) {
		this.metadata = metadata;
		this.name = name;
		this.parent = parent;

		this.additionalFields = additionalFields;
		this.innerMetadataField = innerMetadataField;
	}

	public Metadata getMetadata() {
		return metadata;
	}

	@Override public List<String> ddl() {
		String ourDDL = String.format(
			"CREATE TABLE %s (\n\t%s\n)PRIMARY KEY (\n\t%s\n\t),\nINTERLEAVE IN PARENT %s ON DELETE CASCADE",
			getName(),
			getSpannerFields().stream().map(SpannerField::ddl).collect(Collectors.joining(",\n\t")),
			primaryFields().stream().map(SpannerField::getName).collect(Collectors.joining(",\n\t")),
			getParent().getName()
		);

		List<String> ret = new ArrayList<>();
		ret.add(ourDDL);
		ret.addAll(
			getChildren().stream().flatMap(child -> child.ddl().stream()).collect(Collectors.toList())
		);
		return ret;
	}

	@Override public List<SpannerField> primaryFields() {
		return getSpannerFields().stream()
								 .filter(field -> field.inKey() || field.isVersion())
								 .collect(Collectors.toList());
	}

	@Override public String selectClause() {
		return "SELECT" +
			getSpannerFields().stream().map(SpannerField::asSelect).collect(Collectors.joining(",\n\t"))
			+ " FROM " + getName();
	}

	@Override public List<SpannerField> getSpannerFields() {
		List<SpannerField> spannerFields = getParent().primaryFields()
													  .stream()
													  .map(field -> new SpannerField(this, field))
													  .collect(Collectors.toList());
		spannerFields.addAll(innerMetadataField.getSpannerFields());
		spannerFields.addAll(additionalFields);
		return spannerFields;
	}

	@Override public String getName() {
		return getParent().getName() + "_" + name;
	}

	public SpannerTable getParent() {
		return parent;
	}

	@Override public String keyFilterClause(int index) {
		return null;
	}

	public String asSelect() {
		String fieldsClause = Stream.concat(
			Stream.of(innerMetadataField.asSelect()),
			additionalFields.stream().filter(field -> !field.isInherited())
							.map(SpannerField::asSelect)
							.map(fieldString -> String.format("`%s`.%s", getName(), fieldString))
		).collect(Collectors.joining(",\n\t"));
		String template = " `%s`.%s = `%s`.%s ";
		String whereClause = getParent().primaryFields().stream()
										.map(field -> String.format(
											template,
											getParent().getName(),
											field.getName(),
											getName(),
											field.getName()))
										.collect(Collectors.joining(" AND "));
		return String.format(
			" ARRAY(\n\tSELECT AS STRUCT %s FROM %s WHERE %s \n) AS %s",
			fieldsClause, getName(), whereClause, this.innerMetadataField.getName()
		);
	}

	public List<ChildTable> getChildren() {
		return metadata.getMetadataFields()
					   .stream()
					   .flatMap(field -> field.getChildTables().stream())
					   .collect(Collectors.toList());
	}

	@Override public Stream<Selectable> getSelectables() {
		return getSpannerFields().stream()
								 .map(field -> (Selectable) field); // TODO - not sure why it chokes without the map
	}
}
