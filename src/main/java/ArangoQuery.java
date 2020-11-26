import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.Protocol;
import com.arangodb.entity.BaseDocument;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class ArangoQuery {
    private static String arangoHost;
    private static String collectionName;
    private static String dbName;
    private static String query;
    private static int arangoPort;
    private static int betweenThreads;
    private static int betweenLoops;

    private static long ttl;
    private static Protocol protocol;
    private static int threadCount;
    private static int loopCount;

    private static ArangoDB arangoDB;

    private static ArrayList<String> keyList = new ArrayList();
    ;

    public static void main(String[] args) throws IOException, InterruptedException {
        Random random = new Random(System.nanoTime());

        FileInputStream stream = new FileInputStream(args[0]);
        Properties properties = new Properties();
        properties.load(stream);

        readProperties(properties);
        printOutProperties(properties);
        initDbConnection();
        getKeyIdList();

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                int localCount = loopCount;
                String threadName = Thread.currentThread().getName();

                while (localCount >= 0) {
                    int randomIndex = random.nextInt(keyList.size() - 1);
                    String randomKey = keyList.get(randomIndex);

                    long before = System.currentTimeMillis();
                    //printEntry(localCount, threadName, before, " before ");
                    BaseDocument doc = arangoDB.db(dbName).collection(collectionName).getDocument(randomKey, BaseDocument.class);
                    long after = System.currentTimeMillis();
                    //printEntry(localCount, threadName, after, " after ");
                    printEntry(localCount, threadName, after - before, " total ", doc.getKey());
                    localCount--;
                }
            }).start();

            Thread.sleep(betweenThreads);
        }
    }

    private static void printEntry(int localCount, String threadName, long time, String msg, String key) {
        System.out.println(threadName + " run " + localCount + msg + time + " key " + key);
    }

    private static void getKeyIdList() {
        try {
            ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, null, null, BaseDocument.class);
            cursor.forEachRemaining(aDocument -> {
                keyList.add(aDocument.getKey());
            });
        } catch (ArangoDBException e) {
            System.err.println("Failed to execute query. " + e.getMessage());
        }

        System.out.println("Finished Building List of Keys");
    }

    private static void initDbConnection() {
        arangoDB = new ArangoDB.Builder()
                .host(arangoHost, arangoPort)
                .useProtocol(protocol)
                .useSsl(true)
                .sslContext(buildSslContext())
                .connectionTtl(ttl)
                .build();
    }

    private static void readProperties(Properties properties) {
        arangoHost = properties.getProperty("arango.host");
        collectionName = properties.getProperty("arango.collectionName");
        dbName = properties.getProperty("arango.dbName");
        query = properties.getProperty("arango.query");
        arangoPort = Integer.parseInt(properties.getProperty("arango.port"));
        betweenThreads = Integer.parseInt(properties.getProperty("sleepBetweenThreads"));
        betweenLoops = Integer.parseInt(properties.getProperty("sleepBetweenLoops"));
        ttl = Long.parseLong(properties.getProperty("arango.ttl"));
        protocol = Protocol.valueOf(properties.getProperty("arango.protocol"));
        threadCount = Integer.parseInt(properties.getProperty("threadCount"));
        loopCount = Integer.parseInt(properties.getProperty("loopCount"));
    }

    private static void printOutProperties(Properties properties) {
        for (Object key : properties.keySet()) {
            System.out.println(key + "=" + properties.getProperty(key.toString()));
        }
    }

    private static SSLContext buildSslContext() {
        return null; //todo
    }
}
