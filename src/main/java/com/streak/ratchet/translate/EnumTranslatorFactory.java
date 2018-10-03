package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;

import static com.streak.ratchet.Utils.erase;

public class EnumTranslatorFactory implements TranslatorFactory<Enum> {

	@Override public Translator<Enum> create(AbstractMetadataField metadataField) {
		return new EnumTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return erase(type).isEnum();
	}
}
