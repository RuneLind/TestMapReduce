package no.companybook.HBaseReader.util;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by IntelliJ IDEA.
 * User: birger
 * Date: 1/20/11
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class Util {
    public static byte[] toBytes(Text text){
        byte [] ret = new byte[text.getLength()];
        byte [] invalid = text.getBytes();
         for(int i=0;i<text.getLength(); i++){
            ret[i]=invalid[i];
        }
        return ret;
    }

    public static byte[] toBytes(BytesWritable writable){

        byte [] ret = new byte[writable.getLength()];
        byte [] invalid = writable.getBytes();
         for(int i=0;i<writable.getLength(); i++){
            ret[i]=invalid[i];
        }
        return ret;

    }
    public static byte[] toBytes(ImmutableBytesWritable writable){

        byte [] ret = new byte[writable.getLength()];
        byte [] invalid = writable.get();

        for(int i=0;i<writable.getLength(); i++){
            ret[i]=invalid[i];
        }
        return ret;

    }
}
