package cn.likegirl.hadoop.config;

import cn.likegirl.hadoop.hbase.HbaseTemplate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HbaseClientConfiguration {
	
	private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";

    private static final String HBASE_ZOOKEEPER_PORT = "hbase.zookeeper.port";

    @Bean
    @ConditionalOnMissingBean(HbaseTemplate.class)
    public HbaseTemplate hbaseTemplate() {
        /*org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_QUORUM, "node235,node238,node240");
        configuration.set(HBASE_ZOOKEEPER_PORT, "2181");*/
        return new HbaseTemplate(configuration());
    }


    @Bean
    @ConditionalOnMissingBean(org.apache.hadoop.conf.Configuration.class)
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_QUORUM, "node235,node239,node240");
        configuration.set(HBASE_ZOOKEEPER_PORT, "2181");
        return  configuration;
    }

    @Bean
    @ConditionalOnMissingBean(Connection.class)
    public Connection connection() throws IOException {
        return ConnectionFactory.createConnection(configuration());
    }

}
