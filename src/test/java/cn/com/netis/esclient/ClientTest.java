package cn.com.netis.esclient;

import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import java.net.InetAddress;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import java.util.Map;
import java.util.Date;
import static org.elasticsearch.common.xcontent.XContentFactory.*;


public class ClientTest {

    @Test
    public void testClient() throws Exception {
        // on startup
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
            "}";

        IndexResponse response = client.prepareIndex("testdata", "tom")
                .setSource(json, XContentType.JSON)
                .get();

        // Index name
        String _index = response.getIndex();
        System.out.println("index " + _index);
        // Type name
        String _type = response.getType();
        System.out.println("type " + _type);
        // Document ID (generated or not)
        String _id = response.getId();
        System.out.println("id " + _id);
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        System.out.println("version " + _version);
        // status has stored current instance statement.
        RestStatus status = response.status();
        System.out.println(status.toString());

        GetResponse getResponse = client.prepareGet("testdata", "tom", "1").get();
        System.out.println(getResponse.getIndex());
        System.out.println(getResponse.isExists());
    }

    @AfterClass
    static public void finish() throws Exception {
        client.close();
    }
    
    @BeforeClass
    static public void init() throws Exception {
        // on startup
        Settings settings = Settings.builder()
                .put("cluster.name", "tomtom").build();

        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

    }

    @Test
    public void testIndex() throws Exception {
        XContentBuilder builder = jsonBuilder()
            .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
            .endObject();
        final String json = builder.string();

        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                .setSource(json, XContentType.JSON)
                .get();
        
        // Index name
        String _index = response.getIndex();
        System.out.println("index " + _index);
        // Type name
        String _type = response.getType();
        System.out.println("type " + _type);
        // Document ID (generated or not)
        String _id = response.getId();
        System.out.println("id " + _id);
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        System.out.println("version " + _version);
        // status has stored current instance statement.
        RestStatus status = response.status();
        System.out.println(status.toString());
    }

    @Test
    public void testUpdate() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter");
        updateRequest.type("tweet");
        updateRequest.id("1");
        updateRequest.doc(jsonBuilder()
                .startObject()
                    .field("user", "kkk")
                .endObject());
        client.update(updateRequest).get();
    }

    @Test
    public void testDelete() throws Exception {
        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();
    }

    @Test
    public void testGet() throws Exception {
        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getSourceAsString());
    }

    private static TransportClient client;
}
