import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class MyTest {
    @Test
    public void test1() throws Exception{
        Configuration configuration=new Configuration();
        configuration.set( "fs.defaultFS","hdfs://hadoop1:9000" );
        FileSystem fs=FileSystem.get(configuration);
        System.out.println(fs);
//    fs.copyFromLocalFile(new Path("d:/test.txt"),new Path("/"));

//        fs.copyToLocalFile(new Path("/test.txt"), new Path("c:/vvvsssvb.txt"));
//        fs.mkdirs(new Path("/LJ"));
//        fs.rename(new Path("/LJ"),new Path("/LJJ"));
//        fs.delete(new Path("/LJJ"),true);
//        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
//        while (locatedFileStatusRemoteIterator.hasNext()){
//            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
//            System.out.println(next.getPath().getName());
//            System.out.println(next.getLen());
//            System.out.println(next.getBlockLocations());
//            System.out.println(next.getPermission());
//        }
//        FileInputStream     fileInputStream=new FileInputStream(new File("d:/test2.txt"));
////        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/ddd"));
////        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);
        FSDataInputStream open = fs.open(new Path("/test.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("c:/asas.txt"));
    IOUtils.copyBytes(open,fileOutputStream,configuration);


        fs.close();

    }


    @Test
    public void test() throws Exception{
        int a1=0x7FFFFFFF; //正数的最大值
        int b2=0x80000000; //最大的负数 即是负零
        int b1=0xFFFFFFFF;//-1


        System.out.println(a1);
        System.out.println(b2);
        System.out.println(b1); //打印补码


        int c1=a1&b1; //运算的时候是原码
        int c2=a1&b2;
        System.out.println("c1="+c1); //负数变成了正数
        System.out.println("c2="+c2);





    }
}
