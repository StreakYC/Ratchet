package com.streak.ratchet;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.NotNull;
import com.streak.ratchet.schema.SpannerField;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Calculated fields are fields that exist in Spanner, but are the result of a function in Java
 */
public class MetadataCalculatedField extends AbstractMetadataField {
	private final Method method;

	public MetadataCalculatedField(Method method, Metadata metadata) throws IllegalAccessException {
		super(metadata);

		isKey = null != method.getAnnotation(Key.class);
		isTip = false;
		isVersion = false;
		notNull = null != method.getAnnotation(NotNull.class) || isKey;
		this.method = method;
		this.name = method.getName();
		type = (method.getReturnType().isPrimitive()) ?
			PRIMITIVE_TO_WRAPPER.get(method.getReturnType()) :
			method.getGenericReturnType();

		method.setAccessible(true);
		getter = MethodHandles.lookup().unreflect(method);

		translator = Configuration.INSTANCE.getTranslator(this);
	}

	@Override protected List<SpannerField> makeSpannerFields() {
		return translator.asSpannerFields();
	}

	@Override public void set(Object instance, Object value) throws Throwable {
		// setting doesn't do anything for calculated fields
	}
}
