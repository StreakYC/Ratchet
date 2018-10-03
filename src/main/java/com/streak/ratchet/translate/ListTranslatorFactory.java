package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;
import java.util.List;

import static com.streak.ratchet.Utils.erase;

public class ListTranslatorFactory implements TranslatorFactory<List<?>> {

	@Override public Translator<List<?>> create(AbstractMetadataField metadataField) {
		return new ListTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return List.class.isAssignableFrom(erase(type));
	}
}
