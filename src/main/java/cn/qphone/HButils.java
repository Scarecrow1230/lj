package cn.qphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HButils {
    private final static String ZK_KEY="hbase.zookeeper.quorum";
    private final static String ZK_VALUE="hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private static Connection connection;
    private static Configuration configuration;


    static {
       configuration = new Configuration();
        configuration.set(ZK_KEY, ZK_VALUE);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

        public static HBaseAdmin getAdmin() {

            try {
                //1. 判断connection是否关闭
                if (connection.isClosed()) connection = ConnectionFactory.createConnection(configuration);
                //2. 保证connection非空且没有关闭
                //1.3 获取到admin
                return (HBaseAdmin) connection.getAdmin();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

    // 获取admin对象
    public static Table getTable() {
        return getTable("ns1:user");
    }

    public static Table getTable(String tName){
        try {
            //1. 判断connection是否关闭
            if (connection.isClosed()) connection = ConnectionFactory.createConnection(configuration);
            //2. 保证connection非空且没有关闭
            //1.3 获取到默认的user表
            if (getAdmin().tableExists(TableName.valueOf(tName)))
                return connection.getTable(TableName.valueOf(tName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static  void  close(Connection connection, Admin admin){
            try {
        if(connection!=null){
                connection.close();
        }
        if (admin!=null){
            admin.close();
        }
        } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public  static void  close(Admin admin){
            try {
        connection.close();
        if(admin!=null){

                admin.close();

            } }catch (IOException e) {
                e.printStackTrace();
            }
        }
        }




