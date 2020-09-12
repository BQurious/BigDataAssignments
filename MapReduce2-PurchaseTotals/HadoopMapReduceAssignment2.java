/**
 * @author AswinB
 *
 * Hadoop MapReduce assignment 2 - bill total
 * Created on hadoop version 3.2.1, java v.1.8.0_161 (Oracle/Sun)
 * Lib files included:
 *   hadoop-3.2.1\share\hadoop\common\hadoop-common-3.2.1.jar
 *   hadoop-3.2.1\share\hadoop\mapreduce\hadoop-mapreduce-client-core-3.2.1.jar
 *   hadoop-3.2.1\share\hadoop\common\lib\commons-cli-1.2.jar
 * 
 * Calculates the total bill for multiple buyers/users
 * Input file is expected to be CSV formatted with fields: <username>,<itemPrice>,<quantity>
 */
package hadoopmapreduceassignment2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopMapReduceAssignment2 {

    static String jobID = "billTotal";

    /**
     * @param args the input & output files, input file should be csv (buyerName, pricePer, qty)
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Arguments required: InputFile OutputFile");
            System.exit(1);
        }
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        
        Job jobObj = Job.getInstance(config);
        jobObj.setJarByClass(HadoopMapReduceAssignment2.class);
        jobObj.setMapperClass(MapForBillTotal.class);
        jobObj.setReducerClass(ReduceForBillTotal.class);
        jobObj.setOutputKeyClass(Text.class);
        jobObj.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(jobObj, input);
        FileOutputFormat.setOutputPath(jobObj, output);
        
        System.exit(jobObj.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForBillTotal extends Mapper<LongWritable, Text, Text, FloatWritable> {

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split(",");
            float itemTotal = Float.parseFloat(cols[1]) * Float.parseFloat(cols[2]);
            con.write(new Text(cols[0]), new FloatWritable(itemTotal));
            //return <shoppingList, someItem>
        }
    }

    public static class ReduceForBillTotal extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        public void reduce(Text buyerName, Iterable<FloatWritable> itemsTotals, Context con) throws IOException, InterruptedException {
            float billTotal = 0;
            for (FloatWritable singleItemTotal : itemsTotals) {
                billTotal += singleItemTotal.get();
            }
            con.write(buyerName, new FloatWritable(billTotal));
        }
    }

}
