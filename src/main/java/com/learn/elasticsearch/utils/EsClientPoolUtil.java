package com.learn.elasticsearch.utils;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author ChengRuimin
 * @date: 2019/9/18 15:55
 * @description: es连接池工具类
 */
@Component
public class EsClientPoolUtil {
    /**
     * 对象池配置类（其中可以添加配置）
     */
    private static GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    //默认初始化配置 maxTotal 8，表示池中最多存在8个client
    {
        poolConfig.setMaxTotal(8);
    }

    @Autowired
    private EsClientPoolFactory esClientPoolFactory;
    /**
     * 生成对象池
     */
    private static GenericObjectPool<TransportClient> clientPool;

    public static EsClientPoolUtil esClientPoolUtil;

    @PostConstruct
    public void init() {
        esClientPoolUtil = this;
        clientPool = new GenericObjectPool<>(esClientPoolFactory);
    }

    /**
     * 获得对象
     * @return TransportClient
     * @throws Exception 获取对象异常
     */
    public static TransportClient getClient() throws Exception {
        return clientPool.borrowObject();
    }

    /**
     * 归还对象
     * @param transportClient client对象
     */
    public static void retrunClient(TransportClient transportClient) {
        clientPool.returnObject(transportClient);
    }
}
