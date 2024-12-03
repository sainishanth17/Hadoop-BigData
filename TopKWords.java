import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class TopKWords {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                word.set(parts[0]);
                IntWritable count = new IntWritable(Integer.parseInt(parts[1]));
                context.write(word, count);
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, List<String>> topKMap = new TreeMap<>(Collections.reverseOrder());
        private static final int K = 10; // Change this to your desired k value

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Add to TreeMap
            topKMap.computeIfAbsent(sum, x -> new ArrayList<>()).add(key.toString());

            // Maintain only top K
            if (topKMap.size() > K) {
                topKMap.remove(topKMap.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : topKMap.entrySet()) {
                for (String word : entry.getValue()) {
                    context.write(new Text(word), new IntWritable(entry.getKey()));
                }
            }
        }
    }

    public static class GlobalMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                context.write(new IntWritable(Integer.parseInt(parts[1])), new Text(parts[0]));
            }
        }
    }

    public static class GlobalReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private TreeMap<Integer, List<String>> topKGlobalMap = new TreeMap<>(Collections.reverseOrder());
        private static final int K = 10; // Change this to your desired k value

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text word : values) {
                topKGlobalMap.computeIfAbsent(key.get(), x -> new ArrayList<>()).add(word.toString());
            }

            // Maintain only top K
            while (topKGlobalMap.size() > K) {
                topKGlobalMap.remove(topKGlobalMap.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : topKGlobalMap.entrySet()) {
                for (String word : entry.getValue()) {
                    context.write(new Text(word), new IntWritable(entry.getKey()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) { // Expecting three arguments
            System.err.println("Usage: TopKWords <input path> <output path for job1> <output path for job2>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // First Job
        Job job1 = Job.getInstance(conf, "Top K Words");
        job1.setJarByClass(TopKWords.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(TopKReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setNumReduceTasks(2);

        // Run first job
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Second Job
        Job job2 = Job.getInstance(conf, "Global Top K Words");
        job2.setJarByClass(TopKWords.class);
        job2.setMapperClass(GlobalMapper.class);
        job2.setReducerClass(GlobalReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        // Set input for job2 (this will typically point to the output of job1)
        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input from job1's output
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Adjust the output path

        job2.setNumReduceTasks(1);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
