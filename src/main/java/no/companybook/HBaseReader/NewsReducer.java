package no.companybook.HBaseReader;

/**
 * Created by IntelliJ IDEA.
 * User: Rune
 * Date: 1/28/11
 * Time: 14:46
 * To change this template use File | Settings | File Templates.
 */
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class NewsReducer extends Reducer<Text, Text, Text, Text> {

    //private static final Logger log = Logger.getLogger(NewsReducer.class.getName());

    //public final static Text INFO = new Text("info");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (key != null && values != null) {

            for (Text item : values)
                context.write(key, item);
        }
    }

}
