package cn.qphone;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

    public class HbaseToHdfs extends ToolRunner implements Tool {

        //1. 创建配置对象
        private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
        private final static String HBASE_CONNECT_VALUE = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
        private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
        private final static String HDFS_CONNECT_VALUE = "hdfs://hadoop1:9000";
        private final static String MAPREDUCE_CONNECT_KEY = "mapreduce.framework.name";
        private final static String MAPREDUCE_CONNECT_VALUE = "yarn";

        private Configuration configuration = new Configuration();

        /**
         * 执行方法
         */
        @Override
        public int run(String[] args) throws Exception {
            //1. 获取到作业对象（Job）
            Job job = Job.getInstance(configuration);
            //2 设置你的jar的驱动类型
            job.setJarByClass(HbaseToHdfs.class);
            //3 输入和输出以及作业以及mapper
            TableMapReduceUtil.initTableMapperJob("nsq:t1", new Scan(), HBase2HdfsMapper.class,
                    Text.class, NullWritable.class, job);
            FileOutputFormat.setOutputPath(job, new Path(args[0]));
            //3.6 作业提交（会提示命令行）
            return job.waitForCompletion(true) == true ? 1 : 0;
        }

        /**
         * 设置配置参数的方法
         */
        @Override
        public void setConf(Configuration configuration) {
            configuration.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE);
            configuration.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE);
            configuration.set(MAPREDUCE_CONNECT_KEY, MAPREDUCE_CONNECT_VALUE);
            this.configuration = configuration;
        }

        @Override
        public Configuration getConf() {
            return configuration;
        }

        public static void main(String[] args) throws Exception {
            ToolRunner.run(new HbaseToHdfs(), args);
        }

        public static class HBase2HdfsMapper extends TableMapper<Text, NullWritable> {
            private Text k = new Text(); // 在mapredduce是节省资源
            @Override
            protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
                //1. 创建字符串缓冲区
                StringBuffer sb = new StringBuffer();
                //2. 遍历获取到cell
                while(result.advance()) {
                    Cell cell = result.current(); // 获取到当前的kv值
                    String value = new String(CellUtil.cloneValue(cell));
                    sb.append(value).append(",");
                }
                //3. 写出到hdfs
                k.set(sb.toString());
                context.write(k, NullWritable.get());
            }
        }
    }
