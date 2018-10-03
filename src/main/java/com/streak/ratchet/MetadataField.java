package com.streak.ratchet;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.NotNull;
import com.streak.ratchet.Annotations.Tip;
import com.streak.ratchet.Annotations.Version;
import com.streak.ratchet.schema.SpannerField;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.List;

/**
 * Represents one field on the @Table annotated class.  It is the link between metadata world and spanner world
 */
public class MetadataField extends AbstractMetadataField {

	protected Field field;

	public MetadataField(Field field, Metadata metadata) throws IllegalAccessException {
		super(metadata);
		this.field = field;

		field.setAccessible(true);
		setter = MethodHandles.lookup().unreflectSetter(field);
		getter = MethodHandles.lookup().unreflectGetter(field);

		name = field.getName();

		type = (field.getType().isPrimitive()) ? PRIMITIVE_TO_WRAPPER.get(field.getType()) : field.getGenericType();

		isKey = null != field.getAnnotation(Key.class);
		isTip = null != field.getAnnotation(Tip.class) && type.equals(Boolean.class);
		isVersion = null != field.getAnnotation(Version.class) && type.equals(Long.class);
		notNull = null != field.getAnnotation(NotNull.class) || isVersion || isKey;

		translator = Configuration.INSTANCE.getTranslator(this);
	}

	@Override protected List<SpannerField> makeSpannerFields() {
		return translator.asSpannerFields();
	}
}
