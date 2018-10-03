package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;
import com.streak.ratchet.MetadataField;
import com.streak.ratchet.schema.SpannerField;

import java.util.Collections;
import java.util.List;

public abstract class SimpleTranslator<T> implements Translator<T> {
	protected AbstractMetadataField metadataField;

	@Override public List<SpannerField> asSpannerFields() {
		return Collections.singletonList(new SpannerField(metadataField.getTable(),
			metadataField.getName(),
			spannerType(),
			metadataField.isKey,
			metadataField.isTip,
			metadataField.isVersion,
			metadataField.notNull));
	}

	abstract String spannerType();
}
