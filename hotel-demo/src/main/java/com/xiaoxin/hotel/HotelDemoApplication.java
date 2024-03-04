package com.xiaoxin.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@MapperScan("com.xiaoxin.hotel.mapper")
@SpringBootApplication
public class HotelDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(HotelDemoApplication.class, args);
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(){
        // URL
        String serverUrl = "http://192.168.192.129:9200";

        //创建低级别客户端
        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl)).build();

        //创建JSON映射 用于数据的序列化和反序列化
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        //创建ES客户端
        return  new ElasticsearchClient(transport);
    }

}
