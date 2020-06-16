import com.redislabs.redistimeseries.Aggregation;
import com.redislabs.redistimeseries.RedisTimeSeries;
import com.redislabs.redistimeseries.Value;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class JRedisTimeSeriesUserDepositTest {
    static JedisPool pool;
    private static RedisTimeSeries rts;
    private static JedisPoolConfig config;

    private static int connectTimeout = 30000; // milliseconds
    private static int socketTimeout = 20000; // milliseconds
    private static int iter = 500; // iteration count (for long running test)

    private static String host = "localhost"; // Use HA Redis DB e.g. Redis Enterprise for auto-connect test
    private static int port = 6379; // use same port value i.e. port == sslport for SSL connection
    private static int sslport = 6380; // use same port value i.e. port == sslport for SSL connection
    private static String password;

    private static List<String> getRecordFromLine(String line) {
        List<String> values = new ArrayList<String>();
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(",");
            while (rowScanner.hasNext()) {
                values.add(rowScanner.next().trim());
            }
        }
        return values;
    }

    public static void main(String[] args) throws Exception {

        List<List<String>> records = new ArrayList<>();

        Set<Integer> users = new HashSet<>();
        try (Scanner scanner = new Scanner(new File("C:\\Users\\robo\\Downloads\\success_deposits_asc_1715376_min.csv"));) {
            while (scanner.hasNextLine()) {
                List<String> recordFromLine = getRecordFromLine(scanner.nextLine());
                users.add(Integer.valueOf(recordFromLine.get(0)));
                records.add(recordFromLine);
            }
        }

        rts = new RedisTimeSeries(getPoolInstance());

        String key = "deposit";

        long sec = 1000;
        long minute = 60 * sec;
        long hour = 60*minute;
        long day = 24*hour;
        long week = 7*day;
        long month = 30 *day;


        long to =  LocalDateTime.now().toInstant(ZoneOffset.ofHours(2)).toEpochMilli();
        long fromLong = LocalDateTime.now().minusYears(10).toInstant(ZoneOffset.ofHours(2)).toEpochMilli();

        try {

            rts.create(key);

            for (int i=0 ;i<= 50 ; i++) {
                long time = LocalDateTime.now().minusHours(50 - i).toInstant(ZoneOffset.ofHours(2)).toEpochMilli();

                rts.add(key, time , 1);
            }

            //24 hrs
            Value[] daily_agg = rts.range(key, LocalDateTime.now().minusDays(1).toInstant(ZoneOffset.ofHours(2)).toEpochMilli(),to, Aggregation.SUM, day );
            Arrays.stream(daily_agg).forEach(x -> System.out.println("24hrs Key: "+ key + " Value: " + x.getValue()));
            double sumDaily = Arrays.stream(daily_agg).mapToDouble((x->Double.valueOf(x.getValue()))).sum();
            System.out.println("Daily sum is : " +sumDaily);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

        System.out.println(getPoolCurrentUsage());
        /// ... when closing application:
        getPoolInstance().close();
    }

    private static JedisPool getPoolInstance() {
        if (pool == null) {
            JedisPoolConfig poolConfig = getPoolConfig();
            boolean useSsl = port == sslport ? true : false;
            int db = 0;
            String clientName = "JRedisTimeSeriesTest";
            SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            SSLParameters sslParameters = new SSLParameters();
            //HostnameVerifier hostnameVerifier = new SimpleHostNameVerifier(host);
            if (useSsl) {
                pool = new JedisPool(poolConfig, host, port, connectTimeout, socketTimeout, password, db, clientName,
                        useSsl, sslSocketFactory, sslParameters, null);
            } else {
                pool = new JedisPool(poolConfig, host, port, connectTimeout, socketTimeout, password, db, clientName);
            }
        }
        return pool;
    }

    private static JedisPoolConfig getPoolConfig() {
        if (config == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            // Connection testings
            // To be able to run the while idle test Jedis Pool must set the evictor
            // thread (in "general" section). We will also set the pool to be static
            // so no idle connections could get evicted.

            // Send Redis PING on borrow
            // Recommendation (False), reason - additional RTT on the connection exactly
            // when the app needs it, reduces performance.
            jedisPoolConfig.setTestOnBorrow(false);

            // Send Redis PING on create
            // Recommendation (False), reason - password makes it completely
            // redundant as Jedis sends AUTH
            jedisPoolConfig.setTestOnCreate(false);

            // Send Redis PING on return
            // Recommendation (False), reason - the connection will get tested with
            // the Idle test. No real need here. No impact for true as well.
            jedisPoolConfig.setTestOnReturn(false);

            // Send periodic Redis PING for idle pool connections
            // Recommendation (True), reason - test and heal connections while
            // they are idle in the pool.
            jedisPoolConfig.setTestWhileIdle(true);

            // Dynamic pool configuration
            // This is advanced configuration and the suggestion for most use-cases
            // is to leave the pool static
            // If you need your pool to be dynamic make sure you understand the
            // configuration options

            jedisPoolConfig.setMaxIdle(0);
            jedisPoolConfig.setMinIdle(8);
            jedisPoolConfig.setEvictorShutdownTimeoutMillis(-1);
            jedisPoolConfig.setMinEvictableIdleTimeMillis(-1);
            jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(-1);

            // jedisPoolConfig.setJmxEnabled(true);
            // jedisPoolConfig.setJmxNameBase("JRedisTimeSeriesTest");
            // jedisPoolConfig.setJmxNamePrefix("jrtt");

            // Scheduling algorithms (Leave the defaults)

            // Set to true to have LIFO behavior (always returning the most recently
            // used object from the pool). Set to false to have FIFO behavior
            // Recommendation (?) Default value is True and for now is also the
            // recommendation
            jedisPoolConfig.setLifo(true);
            // Returns whether or not the pool serves threads waiting to borrow
            // objects fairly.
            // True means that waiting threads are served as if waiting in a FIFO
            // queue.
            // False ??maybe?? relies on the OS scheduling
            // Recommendation (?) Default value is False and for now is also
            // the recommendation
            jedisPoolConfig.setFairness(false);

            // General configuration
            // This is the application owner part to configure

            // Pool max size
            jedisPoolConfig.setMaxTotal(100);
            // True - will block the thread requesting a connection from the pool
            // until a connection is ready (or until timeout - "MaxWaitMillis")
            // False - will immediately return an error
            jedisPoolConfig.setBlockWhenExhausted(true);
            // The maximum amount of time (in milliseconds) the borrowObject()
            // method should block before throwing an exception when the pool is
            // exhausted and getBlockWhenExhausted() is true.
            // When less than 0, the borrowObject() method may block indefinitely.
            jedisPoolConfig.setMaxWaitMillis(-1L);
            // The following EvictionRun parameters must be enabled (positive
            // values) in order to enable the evictor thread.
            // The number of milliseconds to sleep between runs of the idle object
            // evictor thread.
            // When positive, the idle object evictor thread starts.
            // Recommendation (>0) A good start is 1000 (one second)
            jedisPoolConfig.setTimeBetweenEvictionRunsMillis(1000L);
            // Number of conns to check each eviction run. Positive value is
            // absolute number of conns to check,
            // negative sets a portion to be checked ( -n means about 1/n of the
            // idle connections in the pool will be checked)
            // Recommendation (!=0) A good start is around fifth.
            jedisPoolConfig.setNumTestsPerEvictionRun(-5);

            JRedisTimeSeriesUserDepositTest.config = jedisPoolConfig;

        }

        return config;
    }

    private static String getPoolCurrentUsage() {

        JedisPool jedisPool = getPoolInstance();
        JedisPoolConfig poolConfig = getPoolConfig();

        int active = jedisPool.getNumActive();
        int idle = jedisPool.getNumIdle();
        int total = active + idle;
        String log = String.format(
                "JedisPool: Active=%d, Idle=%d, Waiters=%d, total=%d, maxTotal=%d, minIdle=%d, maxIdle=%d", active,
                idle, jedisPool.getNumWaiters(), total, poolConfig.getMaxTotal(), poolConfig.getMinIdle(),
                poolConfig.getMaxIdle());

        return log;
    }

	/*
	private static class SimpleHostNameVerifier implements HostnameVerifier {
		private String exactCN;
		private String wildCardCN;
		public SimpleHostNameVerifier(String cacheHostname) {
			exactCN = "CN=" + cacheHostname;
			wildCardCN = "CN=*" + cacheHostname;
		}
		public boolean verify(String s, SSLSession sslSession) {
			try {
				String cn = sslSession.getPeerPrincipal().getName();
				return cn.equalsIgnoreCase(wildCardCN) || cn.equalsIgnoreCase(exactCN);
			} catch (SSLPeerUnverifiedException ex) {
				return false;
			}
		}
	}
	*/
}
