package com.learn.elasticsearch.utils;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

/**
 * @author ChengRuimin
 * @date: 2019/9/18 15:06
 * @description: es客户端连接池工厂
 */
@Component
public class EsClientPoolFactory implements PooledObjectFactory<TransportClient> {
    @Value("${elasticsearch.cluster.name}")
    private String clusterName;
    @Value("${elasticsearch.cluster.ip}")
    private String clusterIp;
    @Value("${elasticsearch.cluster.port.node01}")
    private int node01Port;
    @Value("${elasticsearch.cluster.port.node02}")
    private int node02Port;
    @Value("${elasticsearch.cluster.port.node03}")
    private int node03Port;

    /**
     * 生产对象
     * @return
     * @throws Exception
     */
    @Override
    public PooledObject<TransportClient> makeObject() throws Exception {
        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        //创建访问ES服务器的客户端
        TransportClient transportClient = new PreBuiltTransportClient(settings) .addTransportAddresses(
                new TransportAddress(InetAddress.getByName(clusterIp), node01Port),
                new TransportAddress(InetAddress.getByName(clusterIp), node01Port),
                new TransportAddress(InetAddress.getByName(clusterIp), node01Port));
        return new DefaultPooledObject<TransportClient>(transportClient);
    }

    @Override
    public void destroyObject(PooledObject<TransportClient> pooledObject) throws Exception {
        TransportClient transportClient = pooledObject.getObject();
        transportClient.close();
    }

    @Override
    public boolean validateObject(PooledObject<TransportClient> pooledObject) {
        return true;
    }

    @Override
    public void activateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        System.out.println("----------activateObject----------");
    }

    @Override
    public void passivateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        System.out.println("----------passivateObject----------");
    }
}
