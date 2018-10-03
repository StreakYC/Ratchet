package com.streak.ratchet;

import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.streak.ratchet.Annotations.CalculatedField;
import com.streak.ratchet.Annotations.Ignore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;

public class Utils {
	public static Class<?> erase(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		}
		else if (type instanceof ParameterizedType) {
			return (Class<?>) ((ParameterizedType) type).getRawType();
		}
		else if (type instanceof TypeVariable) {
			TypeVariable<?> tv = (TypeVariable<?>) type;
			if (tv.getBounds().length == 0) {
				return Object.class;
			}
			else {
				return erase(tv.getBounds()[0]);
			}
		}
		throw new RuntimeException("not supported: " + type);
	}

	public static Type innerType(Type type) {
		if (!(type instanceof ParameterizedType)) {
			throw new RuntimeException("Must be parameterized! " + type);
		}
		ParameterizedType pt = (ParameterizedType) type;
		Type[] arguments = pt.getActualTypeArguments();
		if (arguments.length != 1) {
			throw new RuntimeException("Must have only one argument!");
		}
		return arguments[0];
	}

	public static Map<String, Field> createFieldsByName(Class<?> klass) {
		Map<String, Field> mutableMap = Maps.newHashMap();
		if (klass.getSuperclass() != null) {
			// We fill this in first so that we can override things
			mutableMap.putAll(createFieldsByName(klass.getSuperclass()));
		}
		for (Field field : klass.getDeclaredFields()) {
			if ((field.getModifiers() & java.lang.reflect.Modifier.FINAL) != java.lang.reflect.Modifier.FINAL
				&& null == field.getAnnotation(Ignore.class)) {
				mutableMap.put(field.getName(), field);
			}
		}
		return ImmutableMap.copyOf(mutableMap);
	}

	public static Map<String, Method> getCalculatedFields(Class<?> klass) {
		Map<String, Method> mutableMap = Maps.newHashMap();
		if (klass.getSuperclass() != null) {
			// We fill this in first so that we can override things
			mutableMap.putAll(getCalculatedFields(klass.getSuperclass()));
		}
		for (Method method : klass.getDeclaredMethods()) {
			if (null != method.getAnnotation(CalculatedField.class)) {
				mutableMap.put(method.getName(), method);
			}
		}
		return ImmutableMap.copyOf(mutableMap);
	}

	public static Object getRaw(Struct struct, int columnIndex) {
		com.google.cloud.spanner.Type type = struct.getColumnType(columnIndex);
		if (struct.isNull(columnIndex)) {
			return null;
		}

		switch (type.getCode()) {
			case BOOL:
				return struct.getBoolean(columnIndex);
			case INT64:
				return struct.getLong(columnIndex);
			case FLOAT64:
				return struct.getDouble(columnIndex);
			case STRING:
				return struct.getString(columnIndex);
			case BYTES:
				return struct.getBytes(columnIndex);
			case TIMESTAMP:
				return struct.getTimestamp(columnIndex);
			case DATE:
				return struct.getDate(columnIndex);
			case ARRAY:
				switch (type.getArrayElementType().getCode()) {
					case BOOL:
						return struct.getBooleanList(columnIndex);
					case INT64:
						return struct.getLongList(columnIndex);
					case FLOAT64:
						return struct.getDoubleList(columnIndex);
					case STRING:
						return struct.getStringList(columnIndex);
					case BYTES:
						return struct.getBytesList(columnIndex);
					case TIMESTAMP:
						return struct.getTimestampList(columnIndex);
					case DATE:
						return struct.getDateList(columnIndex);
					case STRUCT:
						return struct.getStructList(columnIndex);
					default:
						throw new AssertionError("Invalid type " + type);
				}
			default:
				throw new AssertionError("Invalid type " + type);
		}
	}

    public static Class<?> getTypeFromKeyField(Field keyField) {
        Type genericType = keyField.getGenericType();
        Type innerType = innerType(genericType);
        return erase(innerType);
    }

    public static WrappedField getFieldFromClassOrSuper(Class<?> klass, String fieldName) {
        for (Field field : klass.getDeclaredFields()) {
            if (field.getName().equals(fieldName)) {
                return new WrappedField(klass, field);
            }
        }
        if (null != klass.getSuperclass()) {
            return getFieldFromClassOrSuper(klass.getSuperclass(), fieldName);
        }
        throw new RuntimeException("Field not found");
    }

    public static class WrappedField {
        public final Class<?> klass;
        public final Field field;

        public WrappedField(Class<?> klass, Field field) {
            this.klass = klass;
            this.field = field;
        }
    }
}
