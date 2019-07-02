package cn.com.netis.esclient;

import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import java.net.InetAddress;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.action.get.GetResponse;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        // on startup
        Settings settings = Settings.builder()
                .put("cluster.name", "tomtom").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
            "}";

        IndexResponse response = client.prepareIndex("twitter", "tweet")
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

        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").get();
        System.out.println(getResponse.getIndex());
        System.out.println(getResponse.isExists());

    }
}
