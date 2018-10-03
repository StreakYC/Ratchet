package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;

public class DataClassTranslatorFactory implements TranslatorFactory<Object> {

	@Override public Translator<Object> create(AbstractMetadataField metadataField) {
		return new DataClassTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return true;
	}
}
