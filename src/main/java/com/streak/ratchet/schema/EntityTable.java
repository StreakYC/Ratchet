package com.streak.ratchet.schema;

import com.streak.ratchet.Metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityTable implements SpannerTable {
	private final Metadata metadata;

	public EntityTable(Metadata metadata) {
		this.metadata = metadata;
	}

	@Override public List<String> ddl() {
		List<String> ret = ourDdl();
		ret.addAll(
			getChildren().stream().flatMap(child -> child.ddl().stream()).collect(Collectors.toList())
		);
		return ret;
	}

	private List<String> ourDdl() {
		String mainTableDDL = String.format(
			"CREATE TABLE %s (\n\t%s \n) PRIMARY KEY (\n\t%s\n)",
			getName(),
			getSpannerFields().stream().map(SpannerField::ddl).collect(Collectors.joining(",\n\t")),
			primaryFields().stream().map(SpannerField::getName).collect(Collectors.joining(",\n\t")
			)
		);

		String tipConstraintDDL = String.format(
			"CREATE UNIQUE NULL_FILTERED INDEX %sTipConstraint\n ON %s (\n \t%s,\n \t%s\n)",
			getName(), getName(),
			getSpannerFields().stream()
							  .filter(SpannerField::inKey)
							  .map(SpannerField::getName)
							  .collect(Collectors.joining(",\n\t")),
			getSpannerFields().stream().filter(SpannerField::isTip).map(SpannerField::getName).findFirst().orElse("")
		);
		List<String> ret = new ArrayList<>();
		ret.add(mainTableDDL);
		ret.add(tipConstraintDDL);
		return ret;
	}

	public List<ChildTable> getChildren() {
		return metadata.getMetadataFields()
					   .stream()
					   .flatMap(field -> field.getChildTables().stream())
					   .collect(Collectors.toList());
	}

	@Override public String getName() {
		return metadata.getTableName();
	}

	@Override public List<SpannerField> getSpannerFields() {
		return metadata.getMetadataFields()
					   .stream()
					   .flatMap(field -> field.getSpannerFields().stream())
					   .collect(Collectors.toList());
	}

	@Override public List<SpannerField> primaryFields() {
		return getSpannerFields().stream()
								 .filter(field -> field.inKey() || field.isVersion())
								 .collect(Collectors.toList());
	}

	@Override public String selectClause() {
		return "SELECT" +
			getSelectables().map(Selectable::asSelect).collect(Collectors.joining(",\n\t"))
			+ " FROM " + getName();
	}

	@Override public Stream<Selectable> getSelectables() {
		return Stream.concat(getSpannerFields().stream(), getChildren().stream());
	}

	@Override public String keyFilterClause(int index) {
		return getSpannerFields().stream()
								 .filter(SpannerField::inKey)
								 .map(field -> field.asFilter(index))
								 .collect(Collectors.joining(" AND ", "(", ")"));
	}
}
