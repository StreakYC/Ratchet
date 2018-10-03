package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;

import static com.streak.ratchet.Utils.erase;

public class BooleanTranslatorFactory implements TranslatorFactory<Boolean> {

	@Override public Translator<Boolean> create(AbstractMetadataField metadataField) {
		return new BooleanTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return Boolean.class.isAssignableFrom(erase(type));
	}
}
