package com.streak.ratchet.entities;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.NotNull;
import com.streak.ratchet.Annotations.Table;
import com.streak.ratchet.Annotations.Tip;
import com.streak.ratchet.Annotations.Version;

import java.util.List;
import java.util.Objects;

@Table
public class NestedList {
	@Key
	public Long id;

	@Version
	public Long version;

	@Tip
	public Boolean tip;

	public Boolean bool;

	public SubContainer sc;

	@Override public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		NestedList that = (NestedList) o;
		return Objects.equals(id, that.id) &&
			Objects.equals(version, that.version) &&
			Objects.equals(tip, that.tip) &&
			Objects.equals(sc, that.sc);
	}

	@Override public int hashCode() {

		return Objects.hash(id, version, tip, sc);
	}

	public static class SubContainer {
		public @NotNull long onParent;
		public List<Long> longs;
		public List<SubSimple> subData;

		@Override public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SubContainer that = (SubContainer) o;
			return Objects.equals(longs, that.longs) &&
				Objects.equals(subData, that.subData);
		}

		@Override public int hashCode() {

			return Objects.hash(longs, subData);
		}
	}

	public static class SubSimple {
		public Long x;
		public Long y;

		public SubSimple() {}

		public SubSimple(Long x, Long y) {
			this.x = x;
			this.y = y;
		}

		@Override public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SubSimple subSimple = (SubSimple) o;
			return Objects.equals(x, subSimple.x) &&
				Objects.equals(y, subSimple.y);
		}

		@Override public int hashCode() {

			return Objects.hash(x, y);
		}
	}
}
