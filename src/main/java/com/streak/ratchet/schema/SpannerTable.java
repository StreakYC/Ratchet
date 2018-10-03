package com.streak.ratchet.schema;

import java.util.List;
import java.util.stream.Stream;

public interface SpannerTable {
	String getName();

	List<String> ddl();

	List<SpannerField> primaryFields();

	List<SpannerField> getSpannerFields();

	Stream<Selectable> getSelectables();

	String selectClause();

	String keyFilterClause(int index);
}
