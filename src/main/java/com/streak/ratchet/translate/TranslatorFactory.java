package com.streak.ratchet.translate;

import com.streak.ratchet.AbstractMetadataField;

import java.lang.reflect.Type;

/**
 * We are given a destination class that we want to write to.  That usually means
 * a field type, especially since fields are not erased in the same way that variable types
 * are.
 * <p>
 * Can this factory accept it?  If so, we will be asked for a suitible translator.
 * <p>
 * So - why a factory?  Well, sometimes the translators need to know about the
 * destintation class -- especially translateSpannerValueToFieldType(Object,Type) -- we need to
 * be a factory for objects of Type destination.
 * <p>
 * If you hate me now, just wait until I get bored of updating all of the simple ones
 * and create a TranslatorFactoryFactory
 *
 * @param <P>
 */
public interface TranslatorFactory<P> {

	Translator<P> create(AbstractMetadataField metadataField);

	boolean accepts(Type type);
}
