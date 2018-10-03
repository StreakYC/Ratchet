package com.streak.ratchet;

import com.streak.ratchet.translate.BooleanTranslatorFactory;
import com.streak.ratchet.translate.DataClassTranslatorFactory;
import com.streak.ratchet.translate.DateTranslatorFactory;
import com.streak.ratchet.translate.EnumTranslatorFactory;
import com.streak.ratchet.translate.ListTranslatorFactory;
import com.streak.ratchet.translate.LongTranslatorFactory;
import com.streak.ratchet.translate.StringTranslatorFactory;
import com.streak.ratchet.translate.Translator;
import com.streak.ratchet.translate.TranslatorFactory;

import java.util.ArrayList;
import java.util.List;


public class Translators {
	private final List<TranslatorFactory<?>> translatorFactories = new ArrayList<>();

	public Translators() {
		// TODO - at some point we probably want to allow an annotation to select a specific translator
		translatorFactories.add(new ListTranslatorFactory());
		translatorFactories.add(new StringTranslatorFactory());
		translatorFactories.add(new LongTranslatorFactory());
		translatorFactories.add(new BooleanTranslatorFactory());
		translatorFactories.add(new DateTranslatorFactory());
		translatorFactories.add(new EnumTranslatorFactory());
		translatorFactories.add(new DataClassTranslatorFactory());
	}

	public void addTranslatorFactory(TranslatorFactory translatorFactory) {
		translatorFactories.add(1, translatorFactory);
	}

	public <P> Translator<P> get(AbstractMetadataField metadataField) {
		for (TranslatorFactory<?> translatorFactory : translatorFactories) {
			if (translatorFactory.accepts(metadataField.getType())) {
				//noinspection unchecked
				return (Translator<P>) translatorFactory.create(metadataField);
			}
		}
		throw new RuntimeException("No Translator found: " + metadataField);
	}
}
