package com.streak.ratchet;


import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Statement.Builder;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Query {
	private final Metadata metadata;
	private final List<Object> keyFilter = Lists.newArrayList();
	private Builder builder;
	private Boolean hasWhere = false;
	private int index = 0;
	private Statement statement;

	public Query(Class clazz) {
		metadata = Configuration.INSTANCE.getMetadata(clazz);
		builder = Statement.newBuilder(selectClause());
	}

	private String selectClause() {
		return "SELECT " +
			metadata.getMetadataFields()
					.stream()
					.map(AbstractMetadataField::asSelect)
					.filter(Objects::nonNull)
					.collect(Collectors.joining(",\n\t"))
			+ " FROM " + metadata.getTable().getName();
	}

	public Query filterKey(Object key) {
		keyFilter.add(key);
		return this;
	}

	public <T> List<T> execute() throws Throwable {
		return execute(Configuration.INSTANCE.getSpannerClient()
											 .singleUse());
	}

	public <T> List<T> execute(ReadContext readContext) throws Throwable {
		ResultSet resultSet = readContext.executeQuery(asStatement());
		return metadata.instanceFactory().constructFromResultSet(resultSet);
	}

	public Statement asStatement() {
		if (null == statement) {
			statement = addKeyFilterClauses().build();
		}
		return statement;
	}

	private Builder addKeyFilterClauses() {
		if (!keyFilter.isEmpty()) {
			addWhereOrAnd();

			List<String> keyFilterParts = new ArrayList<>();

			for (Object key : keyFilter) {
				keyFilterParts.add(metadata.getTable().keyFilterClause(++index));
				for (AbstractMetadataField field : metadata.getKeyFields()) {
					field.getTranslator()
						 .addValueToStatement(builder, field.getName(), index, key);
				}
			}
			builder.append("(" + String.join(" OR ", keyFilterParts) + ")");
		}

		return builder;
	}

	public Query addWhereOrAnd() {
		if (!hasWhere) {
			builder.append("\n\tWHERE\n");
			hasWhere = true;
		}
		else {
			builder.append(" AND ");
		}
		return this;
	}

	public Query current() {
		addWhereOrAnd();
		builder.append(String.format(" %s = true ", metadata.getIsTipFieldName()));

		return this;
	}

	public Query addMatch(String fieldName, Object value) {
		addWhereOrAnd();
		AbstractMetadataField field = metadata.getMetadataField(fieldName);
		field.getTranslator().addValueToStatement(builder, field.getName(), ++index, value);
		builder.append(field.getSpannerFields().stream()
							.map(sf -> sf.asFilter(index))
							.collect(Collectors.joining(" AND ", "(", ")")));
		return this;
	}

	public <T> Query addMatches(String fieldName, Collection<T> values) {
		addWhereOrAnd();
		AbstractMetadataField field = metadata.getMetadataField(fieldName);
		List<String> filterParts = new ArrayList<>();
		for (T value : values) {
			field.getTranslator().addValueToStatement(builder, field.getName(), ++index, value);
			filterParts.add(
				field.getSpannerFields().stream()
					 .map(sf -> sf.asFilter(index))
					 .collect(Collectors.joining(" AND ", "(", ")")));
		}
		builder.append("(" + String.join(" OR ", filterParts) + ")");
		return this;
	}
}
