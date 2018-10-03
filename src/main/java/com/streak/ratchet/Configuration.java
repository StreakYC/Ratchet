package com.streak.ratchet;

import com.google.cloud.spanner.DatabaseClient;
import com.streak.ratchet.translate.Translator;
import com.streak.ratchet.translate.TranslatorFactory;

import java.lang.reflect.Type;
import java.util.function.Supplier;

public enum Configuration {
	INSTANCE;

	final private Registrar registrar = new Registrar();
	final private Translators translators = new Translators();
	private Supplier<DatabaseClient> spannerProvider =
		() -> {
			throw new RuntimeException("setSpannerProvider first ");
		};

	public DatabaseClient getSpannerClient() {
		return spannerProvider.get();
	}

	public void register(Class<?> clazz) {
		registrar.register(clazz);
	}

	public Metadata getMetadata(Class<?> klass) {
		return registrar.getMetadata(klass);
	}

	public boolean isValid(Class<?> type) {
		return registrar.isValid(type);
	}

	public void setSpannerProvider(Supplier<DatabaseClient> spannerProvider) {
		this.spannerProvider = spannerProvider;
	}

	public void addTranslatorFactory(TranslatorFactory translatorFactory) {
		translators.addTranslatorFactory(translatorFactory);
	}

	public Translator<?> getTranslator(AbstractMetadataField metadataField) {
		return translators.get(metadataField);
	}
}
