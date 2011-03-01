package no.companybook.HBaseReader;

import no.companybook.HBaseReader.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: Rune
 * Date: 2/23/11
 * Time: 12:52
 * To change this template use File | Settings | File Templates.
 */
public class EntityMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static final Logger log = Logger.getLogger(EntityMapper.class.getName());
    public static Configuration configuration = null;
    private HTable news = null;


//    @Override
//    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//
//    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
        configuration = context.getConfiguration();
        news = new HTable(configuration, "news");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //log.info(value.toString());
        //context.write(key, value);

        byte newRowKey[] = Bytes.toBytes("20110101_123"); // parse from value
        ImmutableBytesWritable outputKey = new ImmutableBytesWritable(newRowKey);

        Put out = new Put(outputKey.get());
        byte[] family = Bytes.toBytes("exstr");
        byte[] qualifier = Bytes.toBytes("person");
        byte[] value1 = Bytes.toBytes("{'Jan'}");
        out.add(new KeyValue(outputKey.get(), family, qualifier, value1));

        news.put(out);
        context.progress();
        context.getCounter("news", "inserts").increment(1);

        //context.write(outputKey, out);
    }
}
