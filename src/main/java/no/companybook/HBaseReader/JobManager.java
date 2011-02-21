package no.companybook.HBaseReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobManager
{
    static String hbaseClientConnectionPort = "6000";
    //public static String hbaseMasterNode = "ip-10-235-5-65.eu-west-1.compute.internal"; // "192.168.0.194"; //"ip-10-235-5-65.eu-west-1.compute.internal"; // "192.168.0.194"; // "ip-10-235-5-65.eu-west-1.compute.internal"; //"172.27.27.234"; // "ip-10-235-5-65.eu-west-1.compute.internal"; //"192.168.0.194:6000"; //
    public static String hbaseMasterNode = "172.27.27.220"; // "192.168.0.194"; //"ip-10-235-5-65.eu-west-1.compute.internal"; // "192.168.0.194"; // "ip-10-235-5-65.eu-west-1.compute.internal"; //"172.27.27.234"; // "ip-10-235-5-65.eu-west-1.compute.internal"; //"192.168.0.194:6000"; //
    static String zooKeeperQuorum = hbaseMasterNode;
    public static Configuration configuration = null;


    //static final String NEWS_TABLE_NAME = "companybook.tables.news";

    //    public JobManager(){
    static
    {
        configuration = HBaseConfiguration.create();

        configuration.set("hbase.master", hbaseMasterNode + ":" + hbaseClientConnectionPort);
        configuration.set("hbase.zookeeper.quorum", zooKeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set(" hbase.mapreduce.scan.cachedrows", "500");
        //configuration.set(NEWS_TABLE_NAME, "news");
    }

    public void runJob(Job job) throws ClassNotFoundException, IOException, InterruptedException {

        System.exit(job.waitForCompletion(true) == true ? 0 : 1);
    }

    public static void main(String[] args) throws Exception
    {
        FileSystem fs = FileSystem.get(configuration);
        System.out.println("HOME-DIR " + fs.getHomeDirectory().toString());
        System.out.println("WORK-DIR " + fs.getWorkingDirectory().toString());
        System.out.println("FS TYPE" + FileSystem.class.toString());

        JobManager mrBuilder = new JobManager();

        System.out.println("running news scan");
        mrBuilder.runJob(readNews());
    }

    public static Job readNews() throws IOException
       {
           String name = "NewsScan";
           Job job = new Job(configuration, name);

           job.setJarByClass(JobManager.class);

           TextOutputFormat.setOutputPath(job, new Path("news"));
           job.setOutputFormatClass(TextOutputFormat.class);

           job.setMapperClass(NewsMapper.class);
           job.setReducerClass(NewsReducer.class);
           job.setOutputKeyClass(Text.class);
           job.setOutputValueClass(Text.class);

           job.setNumReduceTasks(12);
           Scan scan = NewsMapper.createScanner();
           scan.setBatch(100);
           scan.setCacheBlocks(true);
           scan.setCaching(400);

           TableMapReduceUtil.initTableMapperJob(
                   "news",
                   scan,
                   NewsMapper.class,
                   Text.class,
                   Text.class,
                   job);

           return job;
       }


//    public static Job readNews() throws IOException
//    {
//        String name = "NewsScan";
//        Job job = new Job(configuration, name);
//
//        job.setJarByClass(JobManager.class);
//
//        TextOutputFormat.setOutputPath(job, new Path("news"));
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        job.setMapperClass(NewsMapper.class);
//        //job.setReducerClass(NewsReducer.class);
//        job.setReducerClass(IdentityTableReducer.class);
//        job.setOutputKeyClass(ImmutableBytesWritable.class);
//        job.setOutputValueClass(Put.class);
//
//        job.setNumReduceTasks(12);
//        Scan scan = NewsMapper.createScanner();
//        scan.setBatch(100);
//        scan.setCacheBlocks(true);
//        scan.setCaching(400);
//
////        scan.setStartRow(Bytes.toBytes("NO0000000989175378"));
////        scan.setStopRow(Bytes.toBytes ("NO0000000989175379"));
//
//        TableMapReduceUtil.initTableReducerJob(
//            //"news_production",
//            "news",
//            IdentityTableReducer.class,
//            job
//        );
//
//        TableMapReduceUtil.initTableMapperJob(
//                        "news",
//                        scan,
//                        NewsMapper.class,
//                        Text.class,
//                        Text.class,
//                        job);
//
//        TableMapReduceUtil.initTableMapperJob(
//                "news",
//                scan,
//                NewsMapper.class,
//                ImmutableBytesWritable.class,
//                Put.class,
//                job);
//
//        return job;
//    }
}

