package cn.lgwen.table;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class EsAdminApi {

    public static void main(String[] args) throws Exception{
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.20.128.210"), 19301))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.20.128.211"), 19301));

        GetIndexResponse response = client.admin().indices().prepareGetIndex().execute().actionGet();
        System.out.println(response.getIndices().length);
        String[] indices = response.getIndices();
        for(String index : indices){
            System.out.println(index);
        }

    }
}
