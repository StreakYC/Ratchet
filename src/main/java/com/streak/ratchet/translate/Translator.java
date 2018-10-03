package com.streak.ratchet.translate;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.streak.ratchet.schema.ChildTable;
import com.streak.ratchet.schema.SpannerField;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This is based on the objectify translator code, and if you can think of a better way of doing it, please, please
 * let me know.
 * <p>
 * The overall problem it is trying to solve - We know we (or worse, might) have work to do, but we don't know the
 * types involved.  Worse again, it needs to be extensible. For example, we don't want to pollute everyone's
 * package with objectify.
 *
 * @param <P> POJO type
 */

public interface Translator<P> {

	// Given a raw object out of Spanner, and a spanner.Type.Code,
	// what value should be written into a field?
	// This is never actually *used* except internally
	P translateSpannerValueToFieldType(Object item, Type innerType);

	P translateSpannerValueToFieldType(Struct struct);

	// If you aren't null, convert P into a spanner save value and bind it.
	default void addValueToMutation(WriteBuilder builder, String name, P value) {}

	// Ugh -- given the @Name to substitue in, and an object,
	// convert the object into a spanner safe version and
	// bind to the statement builder if you can
	void addValueToStatement(Builder builder,
							 String name,
							 int index, P value);

	void addToKeyBuilder(Key.Builder builder, P value);

	/* A list of the raw values that will be written into spanner

	This is only used in ComparableKey to compare two keys.  Are we using key objects that are not hashable, or
	running into other issues?  I do not remember
	 */
	default List<Object> values(P value) {
		return Collections.singletonList(value);
	}

	default List<SpannerField> asSpannerFields() {
		return Collections.emptyList();
	}

	/*
		These are generally used for collections like List<> that involve child tables
	 */
	default Collection<? extends Mutation> childMutations(Object instance) {
		return Collections.emptyList();
	}

	default Collection<? extends WriteBuilder> childMutationsForValue(Object instance, P value) {
		return Collections.emptyList();
	}

	default List<ChildTable> asChildTables() {
		return Collections.emptyList();
	}

}
