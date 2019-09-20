package com.learn.elasticsearch.test;

import com.learn.elasticsearch.utils.EsClientPoolUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * @author ChengRuimin
 * @date: 2019/9/18 14:31
 * @description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class EsTest {

    private TransportClient transportClient;

    @Before
    public void getClient() throws Exception {
        transportClient = EsClientPoolUtil.getClient();
        System.out.println("----------获取连接----------");
    }

    @After
    public void returnClient() {
        EsClientPoolUtil.retrunClient(transportClient);
        System.out.println("----------归还连接----------");
    }

    @Test
    public void addTest() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("title", "JAVA")
                .field("content", "JAVA is a useful language")
                .endObject();
        //默认type为_doc
        IndexResponse response = transportClient.prepareIndex("my_index", "_doc","2")
                .setSource(doc).get();
        System.out.println(response.status());
    }

    @Test
    public void searchTest() {
        GetResponse response = transportClient.prepareGet("my_index","my_type","1")
                .execute().actionGet();
        System.out.println(response.getSourceAsString());
    }

    @Test
    public void deleteTest() {
        DeleteResponse response = transportClient.prepareDelete("my_index", "my_type", "3").get();
        System.out.println(response.status());
    }

    @Test
    public void updateTest() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        request.index("my_index").type("my_type").id("4")
                .doc(XContentFactory.jsonBuilder().startObject().field("title", "PYTHON").endObject());
        UpdateResponse response = transportClient.update(request).get();
        System.out.println(response.status());
    }

    @Test
    public void upsertTest() throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest("my_index", "my_type", "4")
                .source(XContentFactory.jsonBuilder().startObject().field("title", "C++").endObject());
        UpdateRequest updateRequest = new UpdateRequest("my_index", "my_type", "4")
                .doc(XContentFactory.jsonBuilder().startObject().field("title", "PHP").endObject())
                .upsert(indexRequest);
        UpdateResponse response = transportClient.update(updateRequest).get();
        System.out.println(response.status());
    }

    @Test
    public void mgetTest() {
        MultiGetResponse responses = transportClient.prepareMultiGet()
                .add("my_index", "my_type", "1")
                .add("website", "article","1", "2").get();
        for (MultiGetItemResponse item : responses) {
            GetResponse getResponse = item.getResponse();
            if(getResponse != null && getResponse.isExists()) {
                System.out.println(getResponse.getSourceAsString());
            }
        }
    }

    /**
     * 删除通过查询出来的数据
     */
    @Test
    public void deletedByQuery() {
        BulkByScrollResponse response = new DeleteByQueryRequestBuilder(transportClient, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.matchQuery("title", "test")).source("test_index").get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
        System.out.println(response.getStatus());
    }

    /**
     * 查询并更新； painless：elasticsearch 推出的类似 groove 的脚本语言
     */
    @Test
    public void updateByQuery() {
        BulkByScrollResponse response = new UpdateByQueryRequestBuilder(transportClient, UpdateByQueryAction.INSTANCE)
                .filter(QueryBuilders.termQuery("title", "test")).source("test_index").script(new Script(
                        ScriptType.INLINE, "painless", "ctx._source.title = 'this is an apple'",
                        Collections.emptyMap())).get();
        System.out.println(response.getUpdated());
        System.out.println(response.getStatus());
    }

    /**
     * 异步执行（无效？？？）
     */
    @Test
    public void deleteByQueryAsync() throws InterruptedException {
        new DeleteByQueryRequestBuilder(transportClient, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.matchQuery("title", "test")).source("test_index")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    //同步执行-传统情况：A -> B, A wait for B, B get result, B return A, A -> continue
                    //异步执行-回调函数：A -> B, A continue, B get result, B -> A
                    @Override
                    public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                        long deleted = bulkByScrollResponse.getDeleted();
                        System.out.println(deleted);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException("删除失败");
                    }
                });
        /**
         * 由于是异步执行此处不sleep的话程序会直接结束，就看不到效果了
         */
        Thread.sleep(10000);
    }

    /**
     * bulk操作
     * @throws IOException
     */
    @Test
    public void bulkTest() throws IOException {
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        bulkRequestBuilder.add(transportClient.prepareIndex("test_index", "_doc", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("title", "test").endObject()));
        bulkRequestBuilder.add(transportClient.prepareIndex("test_index", "_doc", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("title", "test all").endObject()));
        bulkRequestBuilder.add(transportClient.prepareIndex("test_index", "_doc", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("title", "test-all").endObject()));
        bulkRequestBuilder.add(transportClient.prepareIndex("test_index", "_doc", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("title", "test_all").endObject()));
        BulkResponse responses = bulkRequestBuilder.get();
        if (responses.hasFailures()) {
            throw new RuntimeException("bulk操作失败");
        }
    }

    @Test
    public void bulkProcessTest() {
        BulkProcessor bulkProcessor = BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {
            /**
             * This method is called just before bulk is executed.
             * @param l
             * @param bulkRequest
             */
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                System.out.println("---------- 开始执行bulk ----------");
                bulkRequest.numberOfActions();
            }

            /**
             * This method is called after bulk execution.
             * @param l
             * @param bulkRequest
             * @param bulkResponse
             */
            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println("---------- 结束执行bulk ----------");
            }

            /**
             * This method is called when the bulk failed and raised a Throwable
             * @param l
             * @param bulkRequest
             * @param throwable
             */
            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                System.err.println(throwable.getMessage());
            }
        })
        .setBulkActions(10000)      //We want to execute the bulk every 10000 requests
//        .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))     //We want to flush the bulk every 5mb
//        .setFlushInterval(TimeValue.timeValueSeconds(5))    //We want to flush the bulk every 5 seconds whatever the number of requests
        .setConcurrentRequests(0)       //Set the number of concurrent requests.
//        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();
        bulkProcessor.add(new IndexRequest("my_index", "my_type", "10").source("title", "C"));
        bulkProcessor.add(new DeleteRequest("my_index", "my_type", "1"));
        bulkProcessor.close();
    }

    /**
     * 改变索引，用处在于替换 index 的 mapping（详见笔记 玩转 elasticsearch 7 中的示例）
     */
    @Test
    public void redindexTest() {
        BulkByScrollResponse response = new ReindexRequestBuilder(transportClient, ReindexAction.INSTANCE)
                .source("old_index").destination("new_index_2").filter(QueryBuilders.matchAllQuery()).get();
        System.out.println(response.getStatus());
    }

}
