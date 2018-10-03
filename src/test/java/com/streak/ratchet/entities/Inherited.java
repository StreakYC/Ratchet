package com.streak.ratchet.entities;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.Table;

@Table
public class Inherited extends Simple {
	public Long getVersion() {
		return version;
	}

	@Key
	protected Long version;
}
