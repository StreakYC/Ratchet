package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;
import java.util.Date;

import static com.streak.ratchet.Utils.erase;

public class DateTranslatorFactory implements TranslatorFactory<Date> {

	@Override public Translator<Date> create(AbstractMetadataField metadataField) {
		return new DateTranslator(metadataField);
	}

	@Override public boolean accepts(Type type) {
		return Date.class.isAssignableFrom(erase(type));
	}
}
