package com.streak.ratchet.entities;

import com.streak.ratchet.Annotations.Key;
import com.streak.ratchet.Annotations.Table;
import com.streak.ratchet.Annotations.Tip;
import com.streak.ratchet.Annotations.Version;

@Table
public class Simple {

	public String getIdString() {
		return idString;
	}

	@Key
	private String idString;

	@Version
	public Long idNumber;

	@Tip
	public Boolean isTip;
}
