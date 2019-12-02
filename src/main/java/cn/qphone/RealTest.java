package cn.qphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;

public class RealTest {

    @Test
    public void test() throws Exception{
 //测试表是否存在
//        HBaseAdmin admin = HButils.getAdmin();
//        boolean nsq = admin.tableExists(TableName.valueOf("nsq:t1"));
//        System.out.println(nsq);

        createTable2("nsq:t5","f1" ,"f2");

    }
    @Test
    public void deleteTable() throws Exception{

 DeleteTable("ns3:t1");
    }

    @Test
    public void createNamespace() throws Exception{

        CrateNamespace("jiayou");
    }

    public void createTable2(String tableName,String... str) throws Exception{


        HBaseAdmin admin = HButils.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String s : str) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }



        admin.createTable(hTableDescriptor);


    }
    public  void  DeleteTable(String tablename) throws IOException {
        HBaseAdmin admin = HButils.getAdmin();
        System.out.println(admin.tableExists(tablename));

        admin.disableTable(tablename);

        admin.deleteTable(tablename);
        System.out.println(admin.tableExists(tablename));
    }
    public void CrateNamespace(String  name) throws IOException {
        HBaseAdmin admin = HButils.getAdmin();
        NamespaceDescriptor build = NamespaceDescriptor.create(name).build();
        admin.createNamespace(build);
        NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(name);
        namespaceDescriptor.getName();
    }


}
