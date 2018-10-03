package com.streak.ratchet;

import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class RatchetReader {
	public <T> List<T> loadFromStatement(Class<T> clazz, Statement query) {
		try {
			Metadata metadata = Configuration.INSTANCE.getMetadata(clazz);
			ResultSet resultSet = getReadContext().executeQuery(query);
			return metadata.instanceFactory().constructFromResultSet(resultSet);
		}
		catch (RuntimeException | Error e) {
			throw e;
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	protected ReadContext getReadContext() {
		return Configuration.INSTANCE.getSpannerClient().singleUse();
	}

	@Nullable
	public <T> T load(Class<T> clazz, Object key) {
		try {
			List<T> results = new Query(clazz).filterKey(key).current().execute(getReadContext());
			if (results.size() == 1) {
				return results.get(0);
			}
			return null;
		}
		catch (RuntimeException | Error e) {
			throw e;
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	public <T> List<T> loadAll(Class<T> clazz, Object key) {
		try {
			return new Query(clazz).filterKey(key).execute(getReadContext());
		}
		catch (RuntimeException | Error e) {
			throw e;
		}
		catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
}
