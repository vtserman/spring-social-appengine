package org.springframework.social.connect.appengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.social.connect.ConnectionKey;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;

public class DatastoreUtils {
	
	public static <T> List<T> queryForList(PreparedQuery pq, EntityMapper<T> entityMapper) {
		return queryForList(pq, FetchOptions.Builder.withDefaults(), entityMapper);
	}
	
	public static <T> List<T> queryForList(PreparedQuery pq, FetchOptions fetchOptions, EntityMapper<T> entityMapper) {
		List<T> resultList = new ArrayList<T>();		
		for (Entity entity : pq.asIterable(fetchOptions)) {
			resultList.add(entityMapper.mapEntity(entity));
		}
		return resultList;
	}
	
	public static <T> Map<Key, T> queryForMap(PreparedQuery pq, EntityMapper<T> entityMapper) {
		Map<Key, T> resultMap = new HashMap<Key, T>();
		for (Entity entity : pq.asIterable()) {
			resultMap.put(entity.getKey(), entityMapper.mapEntity(entity));
		}
		return resultMap;
	}
	
	public static String createConnectionKeyName(String userId, ConnectionKey connectionKey) {
		StringBuilder sb = new StringBuilder(connectionKey.getProviderId())
			.append("-").append(userId)
			.append("-").append(connectionKey.getProviderUserId());
		return sb.toString();
	}
	
	public static interface EntityMapper<T> {
		/** Implementations must implement this method to map each {@code Entity} */
		T mapEntity(Entity entity);
	}

	
}
