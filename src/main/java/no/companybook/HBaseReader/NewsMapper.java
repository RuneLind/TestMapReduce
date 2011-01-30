package no.companybook.HBaseReader;

import no.companybook.HBaseReader.util.Util;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Rune
 * Date: 1/28/11
 * Time: 14:02
 * To change this template use File | Settings | File Templates.
 */
public class NewsMapper extends TableMapper<Text, Text> {

    public final static Text TITLE = new Text("title");
    public final static Text ARTICLE_ID = new Text("article_id");
    public final static Text INFO = new Text("info");

    public static Scan createScanner(){
        Scan scan = new Scan();
        scan.addColumn(Util.toBytes(INFO), Util.toBytes(TITLE));
        return  scan;
    }


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        if (key != null && value != null) {
            MapWritable output = createOutput(
                    value.getRow(),
                    value.getValue(Util.toBytes(INFO), Util.toBytes(TITLE))
            );
            if (output.size() > 0) {
                Text outputKey = (Text) output.get(ARTICLE_ID);
                Text title = (Text) output.get(TITLE);
                context.write(outputKey, title);
            }
        }
    }

    public MapWritable createOutput(
            byte[] rowIdAsBytes,
            byte[] titleAsBytes
    ) {

        MapWritable output = new MapWritable();
        if (
                rowIdAsBytes != null &&
                rowIdAsBytes.length > 0 &&
                titleAsBytes != null &&
                titleAsBytes.length > 0 )
        {
            Text article_id = new Text(Bytes.toString(rowIdAsBytes));
            Text title = new Text(Bytes.toString(titleAsBytes));

            output.put(TITLE, title);
            output.put(ARTICLE_ID, article_id);
        }
        return output;
    }

}
