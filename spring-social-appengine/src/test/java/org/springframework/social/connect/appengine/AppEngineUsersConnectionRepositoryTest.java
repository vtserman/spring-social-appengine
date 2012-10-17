package org.springframework.social.connect.appengine;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.social.connect.ApiAdapter;
import org.springframework.social.connect.Connection;
import org.springframework.social.connect.ConnectionData;
import org.springframework.social.connect.ConnectionKey;
import org.springframework.social.connect.ConnectionRepository;
import org.springframework.social.connect.ConnectionSignUp;
import org.springframework.social.connect.ConnectionValues;
import org.springframework.social.connect.DuplicateConnectionException;
import org.springframework.social.connect.NoSuchConnectionException;
import org.springframework.social.connect.NotConnectedException;
import org.springframework.social.connect.UserProfile;
import org.springframework.social.connect.UserProfileBuilder;
import org.springframework.social.connect.support.ConnectionFactoryRegistry;
import org.springframework.social.connect.support.OAuth1ConnectionFactory;
import org.springframework.social.connect.support.OAuth2ConnectionFactory;
import org.springframework.social.oauth1.OAuth1Operations;
import org.springframework.social.oauth1.OAuth1ServiceProvider;
import org.springframework.social.oauth2.AccessGrant;
import org.springframework.social.oauth2.GrantType;
import org.springframework.social.oauth2.OAuth2Operations;
import org.springframework.social.oauth2.OAuth2Parameters;
import org.springframework.social.oauth2.OAuth2ServiceProvider;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.dev.HighRepJobPolicy;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class AppEngineUsersConnectionRepositoryTest {
	private static final class CustomHighRepJobPolicy implements HighRepJobPolicy {
		static int count = 0;

		@Override
		public boolean shouldApplyNewJob(Key entityGroup) {
			// every other new job fails to apply
			return count++ % 2 == 0;
		}

		@Override
		public boolean shouldRollForwardExistingJob(Key entityGroup) {
			// every other exsting job fails to apply
			return count++ % 2 == 0;
		}
	}
	
	private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
      		.setNoStorage(true)
          .setAlternateHighRepJobPolicyClass(CustomHighRepJobPolicy.class));
	//.setDefaultHighRepJobPolicyUnappliedJobPercentage(100));

	private ConnectionFactoryRegistry connectionFactoryRegistry;
	private TestFacebookConnectionFactory connectionFactory;
	private AppEngineUsersConnectionRepository usersConnectionRepository;
	private ConnectionRepository connectionRepository;

	@Before
	public void setUp() {
		helper.setUp();
		connectionFactoryRegistry = new ConnectionFactoryRegistry();
		connectionFactory = new TestFacebookConnectionFactory();
		connectionFactoryRegistry.addConnectionFactory(connectionFactory);
		usersConnectionRepository = new AppEngineUsersConnectionRepository(connectionFactoryRegistry, Encryptors.noOpText(), DatastoreServiceFactory.getDatastoreService());
		if (!getKindPrefix().equals("")) {
			usersConnectionRepository.setKindPrefix(getKindPrefix());
		}
		connectionRepository = usersConnectionRepository.createConnectionRepository("1");
	}
	
	protected String getKindPrefix() {
		return "";
	}

	@After
	public void tearDown() {
		helper.tearDown();
	}
	
	@Test
	public void findUserIdWithConnection() throws EntityNotFoundException {
		insertFacebookConnection();		
		List<String> userIds = usersConnectionRepository.findUserIdsWithConnection(connectionRepository.getPrimaryConnection(TestFacebookApi.class));
		assertEquals("1", userIds.get(0));
	}
	
	
	@Test
	public void findUserIdWithConnectionNoSuchConnection() {
		Connection<TestFacebookApi> connection = connectionFactory.createConnection(new AccessGrant("12345"));
		assertEquals(0, usersConnectionRepository.findUserIdsWithConnection(connection).size());
	}
	
	
	@Test
	public void findUserIdWithConnectionMultipleConnectionsToSameProviderUser() {
		insertFacebookConnection();
		insertFacebookConnectionSameFacebookUser();
		List<String> localUserIds = usersConnectionRepository.findUserIdsWithConnection(connectionRepository
				.getPrimaryConnection(TestFacebookApi.class));
		assertEquals(2, localUserIds.size());
		assertEquals("1", localUserIds.get(0));
		assertEquals("2", localUserIds.get(1));
	}
	
	@Test
	public void findUserIdWithConnectionNoConnection_withWorkingConnectionSignUp() {		
		Connection<TestFacebookApi> connection = connectionFactory.createConnection(new AccessGrant("12345"));
		usersConnectionRepository.setConnectionSignUp(new ConnectionSignUp() {
			public String execute(Connection<?> connection) {
				return "batman";
			}
		});
		List<String> userIds = usersConnectionRepository.findUserIdsWithConnection(connection);
		assertEquals(1, userIds.size());
		assertEquals("batman", userIds.get(0));
	}
	
	@Test
	public void findUserIdWithConnectionNoConnection_withConnectionSignUpReturningNull() {		
		Connection<TestFacebookApi> connection = connectionFactory.createConnection(new AccessGrant("12345"));
		usersConnectionRepository.setConnectionSignUp(new ConnectionSignUp() {
			public String execute(Connection<?> connection) {
				return null;
			}
		});
		List<String> userIds = usersConnectionRepository.findUserIdsWithConnection(connection);
		assertEquals(0, userIds.size());
	}

	@Test
	public void findUserIdsConnectedTo() {
		insertFacebookConnection();
		insertFacebookConnection3();
		Set<String> localUserIds = usersConnectionRepository.findUserIdsConnectedTo("facebook", new HashSet<String>(Arrays.asList("9", "11")));
		assertEquals(2, localUserIds.size());
		assertTrue(localUserIds.contains("1"));
		assertTrue(localUserIds.contains("2"));		
	}

	@Test
	@SuppressWarnings("unchecked")
	public void findAllConnections() {
		connectionFactoryRegistry.addConnectionFactory(new TestTwitterConnectionFactory());
		insertTwitterConnection();
		insertFacebookConnection();
		MultiValueMap<String, Connection<?>> connections = connectionRepository.findAllConnections();
		assertEquals(2, connections.size());
		Connection<TestFacebookApi> facebook = (Connection<TestFacebookApi>) connections.getFirst("facebook");
		assertFacebookConnection(facebook);
		Connection<TestTwitterApi> twitter = (Connection<TestTwitterApi>) connections.getFirst("twitter");
		assertTwitterConnection(twitter);
	}
	
	@Test
	public void findAllConnectionsMultipleConnectionResults() {
		connectionFactoryRegistry.addConnectionFactory(new TestTwitterConnectionFactory());
		insertTwitterConnection();
		insertFacebookConnection();
		insertFacebookConnection2();
		MultiValueMap<String, Connection<?>> connections = connectionRepository.findAllConnections();
		assertEquals(2, connections.size());
		assertEquals(2, connections.get("facebook").size());
		assertEquals(1, connections.get("twitter").size());
	}

	@Test
	public void findAllConnectionsEmptyResult() {
		connectionFactoryRegistry.addConnectionFactory(new TestTwitterConnectionFactory());
		MultiValueMap<String, Connection<?>> connections = connectionRepository.findAllConnections();
		assertEquals(2, connections.size());
		assertEquals(0, connections.get("facebook").size());
		assertEquals(0, connections.get("twitter").size());		
	}

	@Test(expected=IllegalArgumentException.class)
	public void noSuchConnectionFactory() {
		insertTwitterConnection();
		connectionRepository.findAllConnections();	
	}

	@Test
	@SuppressWarnings("unchecked")
	public void findConnectionsByProviderId() {
		connectionFactoryRegistry.addConnectionFactory(new TestTwitterConnectionFactory());
		insertTwitterConnection();
		List<Connection<?>> connections = connectionRepository.findConnections("twitter");
		assertEquals(1, connections.size());
		assertTwitterConnection((Connection<TestTwitterApi>) connections.get(0));
	}
	
	@Test
	public void findConnectionsByProviderIdEmptyResult() {
		assertTrue(connectionRepository.findConnections("facebook").isEmpty());
	}

	@Test
	public void findConnectionsByApi() {
		insertFacebookConnection();
		insertFacebookConnection2();
		List<Connection<TestFacebookApi>> connections = connectionRepository.findConnections(TestFacebookApi.class);
		assertEquals(2, connections.size());
		assertFacebookConnection(connections.get(0));
	}
	
	@Test
	public void findConnectionsByApiEmptyResult() {
		assertTrue(connectionRepository.findConnections(TestFacebookApi.class).isEmpty());
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void findConnectionsToUsers() {
		connectionFactoryRegistry.addConnectionFactory(new TestTwitterConnectionFactory());
		insertTwitterConnection();
		insertFacebookConnection();
		insertFacebookConnection2();
		MultiValueMap<String, String> providerUsers = new LinkedMultiValueMap<String, String>();
		providerUsers.add("facebook", "10");
		providerUsers.add("facebook", "9");
		providerUsers.add("twitter", "1");
		MultiValueMap<String, Connection<?>> connectionsForUsers = connectionRepository.findConnectionsToUsers(providerUsers);
		assertEquals(2, connectionsForUsers.size());
		assertEquals("10", connectionsForUsers.getFirst("facebook").getKey().getProviderUserId());
		assertFacebookConnection((Connection<TestFacebookApi>) connectionsForUsers.get("facebook").get(1));
		assertTwitterConnection((Connection<TestTwitterApi>) connectionsForUsers.getFirst("twitter"));
	}
	
	@Test
	public void findConnectionsToUsersEmptyResult() {
		MultiValueMap<String, String> providerUsers = new LinkedMultiValueMap<String, String>();
		providerUsers.add("facebook", "1");
		assertTrue(connectionRepository.findConnectionsToUsers(providerUsers).isEmpty());
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void findConnectionsToUsersEmptyInput() {
		MultiValueMap<String, String> providerUsers = new LinkedMultiValueMap<String, String>();
		connectionRepository.findConnectionsToUsers(providerUsers);
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void findConnectionByKey() {
		insertFacebookConnection();
		assertFacebookConnection((Connection<TestFacebookApi>) connectionRepository.getConnection(new ConnectionKey("facebook", "9")));
	}
	
	@Test(expected=NoSuchConnectionException.class)
	public void findConnectionByKeyNoSuchConnection() {
		connectionRepository.getConnection(new ConnectionKey("facebook", "bogus"));
	}

	@Test
	public void findConnectionByApiToUser() {
		insertFacebookConnection();
		insertFacebookConnection2();	
		assertFacebookConnection(connectionRepository.getConnection(TestFacebookApi.class, "9"));
		assertEquals("10", connectionRepository.getConnection(TestFacebookApi.class, "10").getKey().getProviderUserId());
	}

	@Test(expected=NoSuchConnectionException.class)
	public void findConnectionByApiToUserNoSuchConnection() {
		assertFacebookConnection(connectionRepository.getConnection(TestFacebookApi.class, "9"));
	}
	
	@Test
	public void findPrimaryConnection() {
		insertFacebookConnection();
		assertFacebookConnection(connectionRepository.getPrimaryConnection(TestFacebookApi.class));
	}

	@Test
	public void findPrimaryConnectionSelectFromMultipleByRank() {
		insertFacebookConnection2();
		insertFacebookConnection();
		assertFacebookConnection(connectionRepository.getPrimaryConnection(TestFacebookApi.class));
	}

	@Test(expected=NotConnectedException.class)
	public void findPrimaryConnectionNotConnected() {
		connectionRepository.getPrimaryConnection(TestFacebookApi.class);
	}

	@Test
	public void removeConnections() {
		insertFacebookConnection();
		insertFacebookConnection2();
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();		
		Query query = new Query(getKindPrefix() + "UserConnection")
			.setAncestor(KeyFactory.createKey(getKindPrefix() + "User", "1"))
			.setFilter(FilterOperator.EQUAL.of("providerId", "facebook"));
		int countEntities = ds.prepare(query).countEntities(withLimit(1));
		assertTrue(countEntities != 0);
		connectionRepository.removeConnections("facebook");
		countEntities = ds.prepare(query).countEntities(withLimit(1));
		assertTrue(countEntities == 0);
	}
	
	@Test
	public void removeConnectionsToProviderNoOp() {
		connectionRepository.removeConnections("twitter");
	}

	@Test
	public void removeConnection() {
		insertFacebookConnection();
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();		
		Query query = new Query(getKindPrefix() + "UserConnection")
			.setAncestor(KeyFactory.createKey(getKindPrefix() + "User", "1"))
			.setFilter(FilterOperator.EQUAL.of("providerId", "facebook"));
		int countEntities = ds.prepare(query).countEntities(withLimit(1));
		assertTrue(countEntities != 0);		
		connectionRepository.removeConnection(new ConnectionKey("facebook", "9"));
		countEntities = ds.prepare(query).countEntities(withLimit(1));
		assertTrue(countEntities == 0);		
	}

	@Test
	public void removeConnectionNoOp() {
		connectionRepository.removeConnection(new ConnectionKey("facebook", "1"));
	}

	@Test
	public void addConnection() {
		Connection<TestFacebookApi> connection = connectionFactory.createConnection(new AccessGrant("123456789", null, "987654321", 3600));
		connectionRepository.addConnection(connection);
		Connection<TestFacebookApi> restoredConnection = connectionRepository.getPrimaryConnection(TestFacebookApi.class);
		assertEquals(connection, restoredConnection);	
		assertNewConnection(restoredConnection);
	}
	
	@Test(expected=DuplicateConnectionException.class)
	public void addConnectionDuplicate() {
		Connection<TestFacebookApi> connection = connectionFactory.createConnection(new AccessGrant("123456789", null, "987654321", 3600));
		connectionRepository.addConnection(connection);
		connectionRepository.addConnection(connection);
	}
	
	@Test
	public void updateConnectionProfileFields() {
		connectionFactoryRegistry.addConnectionFactory(new TestTwitterConnectionFactory());		
		insertTwitterConnection();
		Connection<TestTwitterApi> twitter = connectionRepository.getPrimaryConnection(TestTwitterApi.class);
		assertEquals("http://twitter.com/kdonald/picture", twitter.getImageUrl());
		twitter.sync();
		assertEquals("http://twitter.com/kdonald/a_new_picture", twitter.getImageUrl());
		connectionRepository.updateConnection(twitter);
		Connection<TestTwitterApi> twitter2 = connectionRepository.getPrimaryConnection(TestTwitterApi.class);
		assertEquals("http://twitter.com/kdonald/a_new_picture", twitter2.getImageUrl());
	}
	
	@Test
	public void updateConnectionAccessFields() {
		insertFacebookConnection();
		Connection<TestFacebookApi> facebook = connectionRepository.getPrimaryConnection(TestFacebookApi.class);
		assertEquals("234567890", facebook.getApi().getAccessToken());
		facebook.refresh();
		connectionRepository.updateConnection(facebook);
		Connection<TestFacebookApi> facebook2 = connectionRepository.getPrimaryConnection(TestFacebookApi.class);
		assertEquals("765432109", facebook2.getApi().getAccessToken());
		ConnectionData data = facebook.createData();
		assertEquals("654321098", data.getRefreshToken());
	}

	
	
	private void insertTwitterConnection() {
		insertConnection("1", "twitter", "1", 1L, "@kdonald", "http://twitter.com/kdonald",
				"http://twitter.com/kdonald/picture", "123456789", "987654321", null, null);		
	}
	
	private void insertFacebookConnection() {
		insertConnection("1", "facebook", "9", 1L, null, null, null, "234567890", null, "345678901",
				System.currentTimeMillis() + 3600000);
		
	}
	
	private void insertFacebookConnection2() {
		insertConnection("1", "facebook", "10", 2L, null, null, null, "456789012", null, "56789012",
				System.currentTimeMillis() + 3600000);
	}

	private void insertFacebookConnection3() {
		insertConnection("2", "facebook", "11", 2L, null, null, null, "456789012", null, "56789012",
				System.currentTimeMillis() + 3600000);
	}

	private void insertFacebookConnectionSameFacebookUser() {
		insertConnection("2", "facebook", "9", 1L, null, null, null, "234567890", null, "345678901",
				System.currentTimeMillis() + 3600000);
	}
	
	private void insertConnection(String userId, String providerId, String providerUserId, Long rank, String displayName,
			String profileUrl, String imageUrl, String accessToken, String secret, String refreshToken, Long expireTime)  
	{
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		String connectionKeyName = userId + "-" + providerId + "-" + providerUserId;
		// Create a new Entity with the specified kind and key name and user as parent Entity.
		Key userKey = KeyFactory.createKey(getKindPrefix() + "User", userId);		
		Entity userConnection = new Entity(getKindPrefix() + "UserConnection", connectionKeyName, userKey);
		userConnection.setProperty("providerId", providerId);
		userConnection.setProperty("providerUserId", providerUserId);
		userConnection.setProperty("rank", rank);		
		userConnection.setProperty("displayName", displayName);
		userConnection.setProperty("profileUrl", profileUrl);
		userConnection.setProperty("imageUrl", imageUrl);
		userConnection.setProperty("accessToken", accessToken);
		userConnection.setProperty("secret", secret);
		userConnection.setProperty("refreshToken", refreshToken);
		userConnection.setProperty("expireTime", expireTime);
				
		Transaction txn = ds.beginTransaction();
		try {			
			ds.put(txn, userConnection);
			txn.commit();			
		} finally {
			if (txn.isActive()) txn.rollback();
		}
	}
	
	
	private void assertNewConnection(Connection<TestFacebookApi> connection) {
		assertEquals("facebook", connection.getKey().getProviderId());
		assertEquals("9", connection.getKey().getProviderUserId());
		assertEquals("Keith Donald", connection.getDisplayName());
		assertEquals("http://facebook.com/keith.donald", connection.getProfileUrl());
		assertEquals("http://facebook.com/keith.donald/picture", connection.getImageUrl());
		assertTrue(connection.test());
		TestFacebookApi api = connection.getApi();
		assertNotNull(api);
		assertEquals("123456789", api.getAccessToken());
		assertEquals("123456789", connection.createData().getAccessToken());
		assertEquals("987654321", connection.createData().getRefreshToken());
	}

	private void assertTwitterConnection(Connection<TestTwitterApi> twitter) {
		assertEquals(new ConnectionKey("twitter", "1"), twitter.getKey());
		assertEquals("@kdonald", twitter.getDisplayName());
		assertEquals("http://twitter.com/kdonald", twitter.getProfileUrl());
		assertEquals("http://twitter.com/kdonald/picture", twitter.getImageUrl());
		TestTwitterApi twitterApi = twitter.getApi();
		assertEquals("123456789", twitterApi.getAccessToken());		
		assertEquals("987654321", twitterApi.getSecret());
		twitter.sync();
		assertEquals("http://twitter.com/kdonald/a_new_picture", twitter.getImageUrl());
	}

	private void assertFacebookConnection(Connection<TestFacebookApi> facebook) {
		assertEquals(new ConnectionKey("facebook", "9"), facebook.getKey());
		assertEquals(null, facebook.getDisplayName());
		assertEquals(null, facebook.getProfileUrl());
		assertEquals(null, facebook.getImageUrl());
		TestFacebookApi facebookApi = facebook.getApi();
		assertEquals("234567890", facebookApi.getAccessToken());
		facebook.sync();
		assertEquals("Keith Donald", facebook.getDisplayName());
		assertEquals("http://facebook.com/keith.donald", facebook.getProfileUrl());
		assertEquals("http://facebook.com/keith.donald/picture", facebook.getImageUrl());		
	}
	

	
	
	// test facebook provider
	private static class TestFacebookConnectionFactory extends OAuth2ConnectionFactory<TestFacebookApi> {
		public TestFacebookConnectionFactory() {
			super("facebook", new TestFacebookServiceProvider(), new TestFacebookApiAdapter());
		}

	}

	private static class TestFacebookServiceProvider implements OAuth2ServiceProvider<TestFacebookApi> {

		public OAuth2Operations getOAuthOperations() {
			return new OAuth2Operations() {
				public String buildAuthorizeUrl(GrantType grantType, OAuth2Parameters params) {
					return null;
				}

				public String buildAuthenticateUrl(GrantType grantType, OAuth2Parameters params) {
					return null;
				}

				public AccessGrant exchangeForAccess(String authorizationGrant, String redirectUri,
						MultiValueMap<String, String> additionalParameters) {
					return null;
				}

				public AccessGrant refreshAccess(String refreshToken, String scope,
						MultiValueMap<String, String> additionalParameters) {
					return new AccessGrant("765432109", "read", "654321098", 3600);
				}
			};
		}

		public TestFacebookApi getApi(final String accessToken) {
			return new TestFacebookApi() {
				public String getAccessToken() {
					return accessToken;
				}
			};
		}

	}

	public interface TestFacebookApi {
		String getAccessToken();
	}

	private static class TestFacebookApiAdapter implements ApiAdapter<TestFacebookApi> {

		private String accountId = "9";
		private String name = "Keith Donald";
		private String profileUrl = "http://facebook.com/keith.donald";
		private String profilePictureUrl = "http://facebook.com/keith.donald/picture";

		public boolean test(TestFacebookApi api) {
			return true;
		}

		public void setConnectionValues(TestFacebookApi api, ConnectionValues values) {
			values.setProviderUserId(accountId);
			values.setDisplayName(name);
			values.setProfileUrl(profileUrl);
			values.setImageUrl(profilePictureUrl);
		}

		public UserProfile fetchUserProfile(TestFacebookApi api) {
			return new UserProfileBuilder().setName(name).setEmail("keith@interface21.com").setUsername("Keith.Donald")
					.build();
		}

		public void updateStatus(TestFacebookApi api, String message) {

		}

	}

	// test twitter provider
	private static class TestTwitterConnectionFactory extends OAuth1ConnectionFactory<TestTwitterApi> {
		public TestTwitterConnectionFactory() {
			super("twitter", new TestTwitterServiceProvider(), new TestTwitterApiAdapter());
		}
	}

	private static class TestTwitterServiceProvider implements OAuth1ServiceProvider<TestTwitterApi> {
		public OAuth1Operations getOAuthOperations() {
			return null;
		}

		public TestTwitterApi getApi(final String accessToken, final String secret) {
			return new TestTwitterApi() {
				public String getAccessToken() {
					return accessToken;
				}

				public String getSecret() {
					return secret;
				}
			};
		}

	}

	public interface TestTwitterApi {
		String getAccessToken();

		String getSecret();
	}

	private static class TestTwitterApiAdapter implements ApiAdapter<TestTwitterApi> {
		private String accountId = "1";
		private String name = "@kdonald";
		private String profileUrl = "http://twitter.com/kdonald";
		private String profilePictureUrl = "http://twitter.com/kdonald/a_new_picture";

		public boolean test(TestTwitterApi api) {
			return true;
		}

		public void setConnectionValues(TestTwitterApi api, ConnectionValues values) {
			values.setProviderUserId(accountId);
			values.setDisplayName(name);
			values.setProfileUrl(profileUrl);
			values.setImageUrl(profilePictureUrl);
		}

		public UserProfile fetchUserProfile(TestTwitterApi api) {
			return new UserProfileBuilder().setName(name).setUsername("kdonald").build();
		}

		public void updateStatus(TestTwitterApi api, String message) {
		}
	}
}
