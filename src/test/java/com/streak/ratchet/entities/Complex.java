package com.streak.ratchet.entities;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.Table;
import com.streak.ratchet.Annotations.Tip;
import com.streak.ratchet.Annotations.Version;

import java.util.Date;
import java.util.List;

@Table
public class Complex {
	@Key
	public Long id;

	@Version
	public Long version;

	@Tip
	public Boolean tip;

	public static class Subclass {
		public STATUS status;
		public Date thatTime;

		public Subclass(STATUS status) {
			this.status = status;
			thatTime = new Date();
		}
		public Subclass() {}
	}

	public enum STATUS {
		GOOD, BAD
	}

	public List<Subclass> sub;

	public long primitive;
}
