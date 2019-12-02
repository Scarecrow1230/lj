package cn.qphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class TestWork {


    @Test
    public void create() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);



        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();


        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("nsq:t_Student"));

        HColumnDescriptor columnDescriptor = new HColumnDescriptor("data_info");

        tableDescriptor.addFamily(columnDescriptor);

        hBaseAdmin.createTable(tableDescriptor);
    }

    @Test
    public void test() throws Exception{
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("nsq:t_Student"));
        Date date=new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyymmdd HH:MM:SS");
        String format = dateFormat.format(date);
      int v = (int)Math.random() * 100;
        String s = String.valueOf(v);
        String s1="001"+format+s;
        Put put = new Put(Bytes.toBytes(s1));
        put.addColumn("data_info".getBytes(), "name".getBytes(), "lj".getBytes());
        put.addColumn("data_info".getBytes(), "age".getBytes(), "22".getBytes());
        put.addColumn("data_info".getBytes(), "address".getBytes(), "sky".getBytes());

        //2.2 添加
        table.put(put);

    }

    @Test
    public void create3() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);



        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();


        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("nsq:t_course"));

        HColumnDescriptor columnDescriptor = new HColumnDescriptor("data_info");

        tableDescriptor.addFamily(columnDescriptor);

        hBaseAdmin.createTable(tableDescriptor);
    }
    @Test
    public void test3() throws Exception{
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("nsq:t_Student"));
        Date date=new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyymmdd HH:MM:SS");
        String format = dateFormat.format(date);
        int v = (int)Math.random() * 100;
        String s = String.valueOf(v);
        String s1="001"+format+s;
        Put put = new Put(Bytes.toBytes(s1));
        put.addColumn("data_info".getBytes(), "Cname".getBytes(), "math".getBytes());
        put.addColumn("data_info".getBytes(), "Teacher".getBytes(), "lixi".getBytes());


        //2.2 添加
        table.put(put);

    }
    @Test
    public void create2() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);



        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();


        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("nsq:t_student_course"));

        HColumnDescriptor columnDescriptor = new HColumnDescriptor("data_info");

        tableDescriptor.addFamily(columnDescriptor);

        hBaseAdmin.createTable(tableDescriptor);
    }

    @Test
    public void test2() throws Exception{
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("nsq:t_Student"));
        Date date=new Date();




        String s1="001_001";
        Put put = new Put(Bytes.toBytes(s1));
        put.addColumn("data_info".getBytes(), "Sid".getBytes(), "001".getBytes());
        put.addColumn("data_info".getBytes(), "Cid".getBytes(), "001".getBytes());


        //2.2 添加
        table.put(put);

    }

   @Test

        public void scan() throws IOException {
       Configuration configuration = new Configuration();

       configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
       Connection connection = ConnectionFactory.createConnection(configuration);
       Table table = connection.getTable(TableName.valueOf("nsq:t_Student"));


            Scan scan = new Scan();


        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("001"));


        RowFilter rowFilter = new RowFilter(
                CompareFilter.CompareOp.EQUAL,
                binaryComparator
        );


        scan.setFilter(rowFilter);
       ResultScanner resultScanner = table.getScanner(scan);
       Iterator<Result> iterator = resultScanner.iterator();
       while(iterator.hasNext()) {
           Result result = iterator.next();
           while(result.advance()) {
               Cell cell = result.current();
               String s = new String(CellUtil.cloneRow(cell));
               String substring = s.substring(3, 7);
               Scan scan1 = new Scan();


               BinaryComparator binaryComparator1 = new BinaryComparator(Bytes.toBytes(substring));


               RowFilter rowFilte1r = new RowFilter(
                       CompareFilter.CompareOp.EQUAL,
                       binaryComparator
               );

               scan1.setFilter(rowFilter);
               ResultScanner resultScanner1 = table.getScanner(scan);
               Iterator<Result> iterator1 = resultScanner.iterator();
               while(iterator.hasNext()) {
                   Result result1 = iterator.next();
                   while(result.advance()) {
                       //打印
                   }
                   }
           }



       }


    }

    public void scan2() throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("nsq:t_Course"));


        Scan scan = new Scan();


        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("001"));


        RowFilter rowFilter = new RowFilter(
                CompareFilter.CompareOp.EQUAL,
                binaryComparator
        );


        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> iterator = resultScanner.iterator();
        while(iterator.hasNext()) {
            Result result = iterator.next();
            while(result.advance()) {
                Cell cell = result.current();
                String s = new String(CellUtil.cloneRow(cell));
                String substring = s.substring(0,4);
                Scan scan1 = new Scan();


                BinaryComparator binaryComparator1 = new BinaryComparator(Bytes.toBytes(substring));

                RowFilter rowFilte1r = new RowFilter(
                        CompareFilter.CompareOp.EQUAL,
                        binaryComparator
                );
                scan1.setFilter(rowFilter);
                ResultScanner resultScanner1 = table.getScanner(scan);
                Iterator<Result> iterator1 = resultScanner.iterator();
                while(iterator.hasNext()) {
                    Result result1 = iterator.next();
                    while(result.advance()) {
                        //打印
                    }
                }

            }



        }


    }
}


