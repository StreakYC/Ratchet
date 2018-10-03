package com.streak.ratchet;

import com.streak.ratchet.Annotations.NotNull;
import com.streak.ratchet.schema.SpannerField;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

/** Represents a field on the spanner table that is nested in the java object
 *
 */
public class NestedMetadataField extends MetadataField {
	private final AbstractMetadataField parent;

	public NestedMetadataField(AbstractMetadataField parent, Field field) throws IllegalAccessException {
		super(field, parent.getMetadata());
		this.parent = parent;

		isKey = false;
		isTip = false;
		isVersion = false;
		notNull = null != field.getAnnotation(NotNull.class);
	}

	@Override protected List<SpannerField> makeSpannerFields() {
		return translator.asSpannerFields().stream()
				  .peek(field -> field.setPrefix(parent.asPrefix())) // ew ew ew (still)
				  .collect(Collectors.toList());
	}

	@Override public String asPrefix() {
		return this.parent.asPrefix() + super.asPrefix();
	}

	@Override protected String spannerFieldName() {
		return parent.asPrefix() + getName();
	}

	public <E> E get(Object instance) throws Throwable {
	    // This is ugly because we aren't using all of the information in places like the ListTranslator
		try {
			//noinspection unchecked
			return (E) getter.invoke(instance);
		} catch(ClassCastException ignore) {
			//noinspection unchecked
			return (E) getter.invoke((Object)parent.get(instance));
		}
	}
}
