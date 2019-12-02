package cn.qphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;

public class myHabse {
    @Test
    public void test() throws Exception{
   HBaseAdmin admin=HButils.getAdmin();
        System.out.println(admin);
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor);
        }
    }

    @Test
    public void test2() throws Exception{
//1. 配置对象
        Configuration configuration = new Configuration();
        //1.1 有可能会有bug，修改windows/hosts：hbase1：192.168.49.100
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        //1.2 获取到链接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //1.3 获取到admin

        //1.3 获取到admin
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        System.out.println(hBaseAdmin);
    NamespaceDescriptor[] namespaceDescriptors = hBaseAdmin.listNamespaceDescriptors();
     for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
         System.out.println(namespaceDescriptor.getName());
     }
     hBaseAdmin.close();

    }

    @Test
    public void list_namespace_tables() throws  IOException {
        Configuration  configuration = new Configuration();
        //1.1 有可能会有bug，修改windows/hosts：hbase1：192.168.49.100
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        //1.2 获取到链接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //1.3 获取到admin
        //1. 获取到admin对象
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //2. 查询呢
        HTableDescriptor[] tableDescriptors = admin.listTableDescriptorsByNamespace("");
        //3. 遍历
        for(HTableDescriptor tableDescriptor : tableDescriptors) {
            System.out.println(tableDescriptor.getNameAsString());
        }
        admin.close();
    }

    @Test
    public void testInit() throws IOException {
        //1. 配置对象
        Configuration configuration = new Configuration();
        //1.1 有可能会有bug，修改windows/hosts：hbase1：192.168.49.100
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        //1.2 创建了HBaseAdmin对象
       ;
        Connection connection = ConnectionFactory.createConnection(configuration);
        //1.3 获取到admin
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        //1.4 测试
        boolean b = hBaseAdmin.tableExists("nsq:t1");
        System.out.println(b);

//        TableName[] tableNames = hBaseAdmin.listTableNames();
//        System.out.println(tableNames.length);
        NamespaceDescriptor[] namespaceDescriptors = hBaseAdmin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }
    }


    @Test
    public void createNamespace() throws Exception{
        HBaseAdmin admin=HButils.getAdmin();
        //创建namespace
        NamespaceDescriptor lj3 = NamespaceDescriptor.create("lj3").build();
        admin.createNamespace(lj3);

        //查看所以的namespace
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }
        //查看namespace下的表
        HTableDescriptor[] lj2s = admin.listTableDescriptorsByNamespace("lj2");
        for (HTableDescriptor lj2 : lj2s) {
            System.out.println(lj2.getNameAsString());
        }
        //列处所以的表描述器
        TableName[] lj2s1 = admin.listTableNamesByNamespace("lj2");
        for (TableName tableName : lj2s1) {
            System.out.println(lj2s1);

        }
        //删除namespace
        admin.deleteNamespace("lj2");

    }

    //建表
    @Test
    public void createtable() throws Exception{
        HBaseAdmin admin=HButils.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("ns1:t2"));
        HColumnDescriptor myinfo = new HColumnDescriptor("myinfo");
        hTableDescriptor.addFamily(myinfo);

        admin.createTable(hTableDescriptor);

    }


    @Test
    public void listtablename() throws Exception {
        HBaseAdmin admin=HButils.getAdmin();
        //查看表
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            tableName.getNameAsString();
            tableName.getNamespaceAsString();

        }

        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            hTableDescriptor.getNameAsString();

        }
//修改
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf("lj2"));
        HColumnDescriptor hColumnDescriptor = hTableDescriptor.getFamily("f1".getBytes());
        hColumnDescriptor.setVersions(1,5);
        hColumnDescriptor.setBlocksize(123);
        hColumnDescriptor.setInMemory(true);
        hTableDescriptor.modifyFamily(hColumnDescriptor);
        admin.modifyTable("lj2",hTableDescriptor);

        //删除
        TableName tableName = TableName.valueOf("lj");
        if (!admin.tableExists(tableName)) return;
        if (admin.isTableEnabled(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    @Test
    public void dml() throws Exception{
        //增
        Table table=HButils.getTable();
        Put put=new Put("001".getBytes());
        put.addColumn("BBB".getBytes(),"SAS".getBytes(),"sasa".getBytes());
        put.addColumn("BBB".getBytes(),"SAS".getBytes(),"sasa2".getBytes());

        table.put(put);

        //get
        Get get = new Get("003".getBytes());
        Result result = table.get(get);
        while (result.advance()){
            System.out.println(result.getRow());
            Cell current = result.current();
            System.out.println();
        }
    }



    }





