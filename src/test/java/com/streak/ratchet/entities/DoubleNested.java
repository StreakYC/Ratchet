package com.streak.ratchet.entities;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.Table;
import com.streak.ratchet.Annotations.Tip;
import com.streak.ratchet.Annotations.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Table
public class DoubleNested {
	@Key
	public Long id;

	@Version
	public Long version;

	@Tip
	public Boolean tip;

	@Override public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DoubleNested that = (DoubleNested) o;
		return Objects.equals(id, that.id) &&
			Objects.equals(version, that.version) &&
			Objects.equals(sub, that.sub);
	}

	@Override public int hashCode() {

		return Objects.hash(id, version, tip, sub);
	}


	public List<List<Long>> sub = new ArrayList<>();
}
