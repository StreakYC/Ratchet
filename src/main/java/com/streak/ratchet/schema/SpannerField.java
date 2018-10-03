package com.streak.ratchet.schema;

public class SpannerField implements Selectable {
	private final boolean isTip;
	private final boolean isVersion;
	private final boolean inKey;
	private final String name;
	private final String type;
	private final boolean isInherited;
	private final boolean notNull;
	private SpannerTable table;
	private String prefix = "";

	public SpannerField(SpannerTable table,
						String fieldName,
						String spannerType,
						Boolean inKey,
						Boolean isTip,
						Boolean isVersion,
						Boolean notNull) {
		this.table = table;
		this.name = fieldName;
		this.type = spannerType;
		this.inKey = inKey;
		this.isTip = isTip;
		this.isVersion = isVersion;
		this.notNull = notNull;
		isInherited = false;
	}

	public SpannerField(SpannerTable table, SpannerField parentField) {
		this.table = table;
		this.isTip = parentField.isTip;
		this.isVersion = parentField.isVersion;
		this.inKey = parentField.inKey;
		this.name = parentField.name;
		this.type = parentField.type;
		this.prefix = parentField.prefix;
		this.notNull = parentField.notNull;
		isInherited = true;
	}

	public boolean isInherited() {
		return isInherited;
	}

	public String ddl() {
		return String.format("`%s`\t%s%s", getName(), type, notNull() ? "\tNOT NULL" : "");
	}

	public String getName() {
		return prefix + name; // TODO - there has got to be a better system, or at least better names
	}

	public boolean notNull() {
		return notNull;
	}

	public boolean inKey() {
		return inKey;
	}

	public boolean isVersion() {
		return isVersion;
	}

	public boolean isTip() {
		return isTip;
	}

	@Override public String asSelect() {
		return String.format("`%s` as %s", getName(), name);
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String asFilter(int index) {
		return String.format("`%s`.`%s` = @%s_%s", table.getName(), getName(), getName(), index);
	}
}
