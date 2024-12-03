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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class SimpleIDTokenizer {

    public static class CountToIDMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable countWritable = new LongWritable();
        private Text wordWritable = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                wordWritable.set(parts[0]);
                countWritable.set(Long.parseLong(parts[1])); // Using LongWritable
                context.write(wordWritable, countWritable);
            }
        }
    }

    public static class IDReducer extends Reducer<Text, LongWritable, IntWritable, Text> {
        private PriorityQueue<Map.Entry<Text, Long>> countQueue = new PriorityQueue<>(
            (a, b) -> Long.compare(a.getValue(), b.getValue())
        );

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            countQueue.offer(new AbstractMap.SimpleEntry<>(new Text(key), sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Convert priority queue to list for sorting
            List<Map.Entry<Text, Long>> countList = new ArrayList<>(countQueue);
            // Sort by frequency in descending order
            Collections.sort(countList, (a, b) -> Long.compare(b.getValue(), a.getValue()));

            int rank = 1;
            for (Map.Entry<Text, Long> entry : countList) {
                context.write(new IntWritable(rank++), entry.getKey());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SimpleIDTokenizer <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Simple ID Tokenizer");
        job.setJarByClass(SimpleIDTokenizer.class);
        job.setMapperClass(CountToIDMapper.class);
        job.setReducerClass(IDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1); // To maintain order, we use a single reducer
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
