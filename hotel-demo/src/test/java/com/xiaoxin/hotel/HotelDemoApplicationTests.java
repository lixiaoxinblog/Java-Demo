package com.xiaoxin.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.KeywordProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import com.xiaoxin.hotel.pojo.Hotel;
import com.xiaoxin.hotel.pojo.HotelDoc;
import com.xiaoxin.hotel.service.impl.HotelService;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

@SpringBootTest
class HotelDemoApplicationTests {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Autowired
    private HotelService hotelService;

    @Test
    public void createHotelIndex() throws IOException {
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        elasticsearchClient = new ElasticsearchClient(transport);

        //创建索引约束
        HashMap<String, Property> propertyHashMap = new HashMap<>();
        Property keywordProperty = Property.of(p -> p.keyword(b -> b));
        //设置类型我text，并指定分词器为ik_max_word
        Property textProperty = Property.of(p -> p.text(b -> b.analyzer("ik_max_word")));
        //设置类型为keyword，不进行搜索
        Property addressProperty = Property.of(p -> p.keyword(b -> b.index(false)));
        //把约束添加经HashMap中
        propertyHashMap.put("id",keywordProperty);
        propertyHashMap.put("name",textProperty);
        propertyHashMap.put("address",addressProperty);
        //创建一个名为hotel 的索引库 并添加索引约束
        elasticsearchClient.indices().create(b -> b.index("hotel").mappings(builder -> builder.properties(propertyHashMap)));
    }

    @Test
    public void testDeleteMapping() throws IOException {
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        elasticsearchClient = new ElasticsearchClient(transport);

        elasticsearchClient.indices().delete(builder -> builder.index("hotel"));
    }

    @Test
    public void testQueryMapping() throws IOException {
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        elasticsearchClient = new ElasticsearchClient(transport);

        GetIndexResponse hotel = elasticsearchClient.indices().get(builder -> builder.index("hotel"));
        System.out.println(hotel.toString());
    }

    @Test
    public void testInsertDocument() throws IOException {
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        elasticsearchClient = new ElasticsearchClient(transport);

        //创建一个实体类并添加数据
        Hotel hotel = new Hotel();
        hotel.setId(1L);
        hotel.setAddress("重庆巴南区");
        hotel.setName("巴南区人民酒店");

        elasticsearchClient.index(i -> i
                .index("hotel")
                .id(hotel.getId().toString())
                .document(hotel));
    }
    @Test
    public void testGetDocument() throws IOException {
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        elasticsearchClient = new ElasticsearchClient(transport);

        SearchResponse<Hotel> search = elasticsearchClient.search(b -> b.index("hotel")
                .query(q -> q.match(m -> m.field("id")
                        .query("1"))), Hotel.class);

        for (Hit<Hotel> hit: search.hits().hits()) {
            System.out.println(hit.source());
        }
        System.out.println(search.hits().hits().get(0).source());
        System.out.println("###################################");

        GetResponse<Hotel> hotel = elasticsearchClient.get(builder -> builder.id("1").index("hotel"), Hotel.class);
        System.out.println(hotel.source());
    }
    @Test
    public void testUpdateDocument() throws IOException {
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        elasticsearchClient = new ElasticsearchClient(transport);

        Hotel hotel = new Hotel();
        hotel.setId(10L);
        hotel.setAddress("重庆巴南区");
        hotel.setName("巴南区人民高级酒店111");

        elasticsearchClient.update(u-> u
                .id(hotel.getId().toString())
                .index("hotel").doc(hotel).upsert(hotel), Hotel.class);
    }

    @SneakyThrows
    @Test
    public void testBulkDocument(){
        List<Hotel> list = hotelService.list();
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Hotel hotel : list) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            br.operations(op -> op.index(io -> io.index("hotel")
                    .id(hotelDoc.getId().toString()).document(hotelDoc)
            ));
        }
        BulkResponse result = elasticsearchClient.bulk(br.build());
        if (result.errors()){
            for (BulkResponseItem item : result.items()) {
                assert item.error() != null;
                System.out.println(item.error().reason());
            }
        }
    }


}
