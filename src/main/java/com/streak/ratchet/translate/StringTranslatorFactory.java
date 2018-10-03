package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;

import static com.streak.ratchet.Utils.erase;

public class StringTranslatorFactory implements TranslatorFactory<String> {

	@Override public Translator<String> create(AbstractMetadataField metadataField) {
		return new StringTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return String.class.isAssignableFrom(erase(type));
	}
}
