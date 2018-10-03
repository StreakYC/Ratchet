package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;

import static com.streak.ratchet.Utils.erase;

public class LongTranslatorFactory implements TranslatorFactory<Long> {

	@Override public Translator<Long> create(AbstractMetadataField metadataField) {
		return new LongTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return Long.class.isAssignableFrom(erase(type));
	}
}
