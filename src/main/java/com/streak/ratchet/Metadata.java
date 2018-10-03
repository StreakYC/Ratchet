package com.streak.ratchet;

import com.streak.ratchet.Annotations.Table;
import com.streak.ratchet.schema.ChildTable;
import com.streak.ratchet.schema.EntityTable;
import com.streak.ratchet.schema.SpannerField;
import com.streak.ratchet.schema.SpannerTable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.streak.ratchet.Utils.erase;
import static com.streak.ratchet.Utils.innerType;

/**
 * Metadata for a class annotated for Ratchet.
 * <p>
 * Things that relate to the class as a whole go here.  Things that relate to a single field go in MetadataField
 */
public class Metadata {
	private final Map<String, AbstractMetadataField> metadataFieldMap;
	private final String tableName;
	private final Class<?> entityClass;
	private final SpannerTable table;
	public Metadata parentMetadata; // todo

	public Metadata(Class<?> entityClass) throws IllegalAccessException {
		tableName = computeTableName(entityClass);
		this.entityClass = entityClass;
		this.table = new EntityTable(this);
		Map<String, Field> fieldsByName = Utils.createFieldsByName(entityClass);

		metadataFieldMap = new HashMap<>();
		for (Entry<String, Field> entry : fieldsByName.entrySet()) {
			metadataFieldMap.put(entry.getKey(), new MetadataField(entry.getValue(), this));
		}
		for (Entry<String, Method> entry : Utils.getCalculatedFields(entityClass).entrySet()) {
			metadataFieldMap.put(entry.getKey(), new MetadataCalculatedField(entry.getValue(), this));
		}

	}

	public Metadata(AbstractMetadataField metadataField,
					List<SpannerField> additionalFields) {
		InnerMetadataField innerMetadataField = new InnerMetadataField(this, metadataField);
		parentMetadata = metadataField.getMetadata();
		tableName = metadataField.asPrefix().replaceFirst(".$",""); // TODO this is a sign that the formatting is wrong
		table = new ChildTable(this, tableName, metadataField.getTable(), additionalFields, innerMetadataField);
		entityClass = erase(innerType(metadataField.getType())); // not sure if this makes sense.  might be another subclass of metadata (probably)
		metadataFieldMap = new HashMap<>();
        metadataFieldMap.put(metadataField.getName(), innerMetadataField);
	}

	private static String computeTableName(Class<?> klass) {
		if (null != klass.getDeclaredAnnotation(Table.class)) {
			String tableName = klass.getDeclaredAnnotation(Table.class).name();
			if (tableName.isEmpty()) {
				return klass.getSimpleName();
			}
			return tableName;
		}
		return null;
	}

	public boolean isValid() {
		return !getKeyFields().isEmpty() &&
			null != getVersionField() &&
			null != getTipField() &&
			null != tableName;
	}

	public List<AbstractMetadataField> getKeyFields() {
		return metadataFieldMap.values()
							   .stream()
							   .filter(field -> field.isKey)
							   .collect(Collectors.toList());
	}

	public AbstractMetadataField getVersionField() {
		AbstractMetadataField versionField = null;
		for (AbstractMetadataField field : metadataFieldMap.values()) {
			if (field.isVersion) {
				if (versionField != null) {
					throw new RuntimeException(String.format("%s has multiple version fields!", entityClass));
				}
				else {
					versionField = field;
				}
			}
		}
		return versionField;
	}

	private AbstractMetadataField getTipField() {
		AbstractMetadataField tipField = null;
		for (AbstractMetadataField field : metadataFieldMap.values()) {
			if (field.isTip) {
				if (tipField != null) {
					throw new RuntimeException(String.format("%s has multiple tip fields!", entityClass));
				}
				else {
					tipField = field;
				}
			}
		}
		return tipField;
	}

	public Collection<AbstractMetadataField> getMetadataFields() {
		return metadataFieldMap.values();
	}

	public Long getVersion(Object instance) throws Throwable {
		return getVersionField().get(instance);
	}

	public <T> T get(String fieldName, Object instance) throws Throwable {
		//noinspection unchecked
		return (T) metadataFieldMap.get(fieldName).get(instance);
	}

	public String tipConstraint() {
		return this.getTableName() + "TipConstraint";
	}

	public String getTableName() {
		return tableName;
	}

	public List<String> tipConstraintColumns() {
		return getTable().primaryFields().stream().map(SpannerField::getName).collect(Collectors.toList());
	}

	public SpannerTable getTable() {
		return table;
	}

	public void setTip(Object instance, Boolean isTip) throws Throwable {
		if (isTip == Boolean.FALSE) {
			throw new IllegalArgumentException("tip must be true or null");
		}
		getTipField().set(instance, isTip);
	}

	public String getIsTipFieldName() {
		return getTipField().getName();
	}

	public void setVersion(Object instance, long versionId) throws Throwable {
		set(getVersionFieldName(), instance, versionId);
	}

	public void set(String fieldName, Object instance, Object value) throws Throwable {
		metadataFieldMap.get(fieldName).set(instance, value);
	}

	public String getVersionFieldName() {
		return getVersionField().getName();
	}

	public <E> E newInstance() throws IllegalAccessException, InstantiationException {
	    // Objectify has a concept of the constructors for types, and we should probably steal that.
        // Translators most likely know enough to create the objects, which would solve the issues where we can't
        // set some values as well.
		if (entityClass.equals(List.class)) {
			return (E) ArrayList.class.newInstance();
		}
		return (E) entityClass.newInstance();
	}

	public InstanceFactory instanceFactory() {
		return new InstanceFactory(this);
	}

	public AbstractMetadataField getMetadataField(String fieldName) {
		return metadataFieldMap.get(fieldName);
	}
}
