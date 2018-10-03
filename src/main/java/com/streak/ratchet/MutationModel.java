package com.streak.ratchet;

public class MutationModel {
	public final boolean isDelete;
	public final Object instance;

	private MutationModel(boolean isDelete, Object instance) {
		this.isDelete = isDelete;
		this.instance = instance;
	}

	public static MutationModel forUpdate(Object instance) {
		return new MutationModel(false, instance);
	}

	public static MutationModel forDelete(Object instance) {
		return new MutationModel(true, instance);
	}
}
