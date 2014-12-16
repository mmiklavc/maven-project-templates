package com.hortonworks;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void configure(JobConf job) {
            // Path cacheFileArchive = null;
            // try {
            // cacheFileArchive = DistributedCache.getLocalCacheArchives(job)[0];
            // } catch (IOException e) {
            // System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(e));
            // }
            String symlink = job.get("symlink");
            String containingFile = job.get("containingFile");
            String innerFile = job.get("innerFile") != null ? job.get("innerFile") : "";
            File f = new File("./" + containingFile + innerFile);
            System.out.println("symlink?" + symlink);
            if (!symlink.equals("")) {
                f = new File("./" + symlink + innerFile);
            }
            File dir = new File("./");
            for (File item : dir.listFiles()) {
                System.out.println("name: " + item.getName());
            }
            System.out.println("cache archive: " + f.toString());
            System.out.println("file exists: " + f.exists());
        }

        // private void parseSkipFile(Path patternsFile) {
        // try {
        // BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
        // String pattern = null;
        // while ((pattern = fis.readLine()) != null) {
        // patternsToSkip.add(pattern);
        // }
        // } catch (IOException ioe) {
        // System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : "
        // + StringUtils.stringifyException(ioe));
        // }
        //
        // }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class ReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        final String inputPath = args[0];
        final String outputPath = args[1];
        final String cacheFile = args[2];
        if (args.length == 4) {
            conf.set("innerFile", "/" + args[3]);
        }

        System.out.println("Running word count");
        System.out.println("Input Path=" + inputPath);
        System.out.println("Output Path=" + outputPath);
        System.out.println("Setting up Job Conf");
        conf.setJobName("wordcount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(ReduceClass.class);
        
        String symlink = cacheFile.substring(cacheFile.indexOf('#') + 1);
        conf.set("symlink", symlink);

        String containingFile = "";
        if (!symlink.equals("")) {
            containingFile = cacheFile.substring(cacheFile.lastIndexOf('/') + 1, cacheFile.indexOf('#') - 1);
        } else {
            containingFile = cacheFile.substring(cacheFile.lastIndexOf('/') + 1);
        }
        conf.set("containingFile", containingFile);
        // hdfs://namenode:port/lib.so.1#lib.so
        DistributedCache.addCacheArchive(new Path(cacheFile).toUri(), conf);
        conf.set("mapred.create.symlink", "yes");
        System.out.println("archive added");
//         DistributedCache.addCacheFile(new Path(cacheFile).toUri(), conf);
        // DistributedCache.addCacheFile(new URI(cacheFile), conf);
        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        JobClient.runJob(conf);
        return 0;
    }
}
