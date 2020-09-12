/**
 * @author AswinB
 * 
 * Hadoop MapReduce assignment (word count)
 * Created on hadoop version 3.2.1, java v.1.8.0_161 (Oracle/Sun)
 * Lib files included:
 *   hadoop-3.2.1\share\hadoop\common\hadoop-common-3.2.1.jar
 *   hadoop-3.2.1\share\hadoop\mapreduce\hadoop-mapreduce-client-core-3.2.1.jar
 *   hadoop-3.2.1\share\hadoop\common\lib\commons-cli-1.2.jar
 */
package hadoopmapreduceassignment;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopMapReduceAssignment {

    static String separator = " ";
    static String jobID = "wordcount";
    
    /**
     * @param args the input & output files
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2)
        {
            System.out.println("Arguments required: InputFile OutputFile");
            System.exit(1);
        }
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job jobObj = Job.getInstance(config, jobID);
        jobObj.setJarByClass(HadoopMapReduceAssignment.class);
        jobObj.setMapperClass(MapForWordCount.class);
        jobObj.setReducerClass(ReduceForWordCount.class);
        jobObj.setOutputKeyClass(Text.class);
        jobObj.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobObj, input);
        FileOutputFormat.setOutputPath(jobObj, output);
        System.exit(jobObj.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(separator);
            for (String word : words) {
                Text outputKey = new Text(word.toLowerCase().trim());
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }

}
