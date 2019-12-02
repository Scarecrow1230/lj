package cn.qphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Arrays;

public class mmm {
    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        //1.1 有可能会有bug，修改windows/hosts：hbase1：192.168.49.100
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        //1.2 获取到链接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //1.3 获取到admin

        //1.3 获取到admin
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        System.out.println(hBaseAdmin);
        HTableDescriptor tableDescriptor  = hBaseAdmin.getTableDescriptor("nsq:t1".getBytes());
        System.out.println(tableDescriptor);
//        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
//            System.out.println(namespaceDescriptor.getName());
//        }
//        hBaseAdmin.close();
    }
}
