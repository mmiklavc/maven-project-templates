package com.hortonworks;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountExtraDep {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] localPaths = context.getCacheFiles();
            for (URI uri : localPaths) {
                System.out.println("uri=" + uri.toString());
            }
            String symlink = conf.get("symlink");
            String containingFile = conf.get("containingFile");
            String innerFile = conf.get("innerFile") != null ? conf.get("innerFile") : "";
            File f = new File("./" + containingFile + innerFile);
            System.out.println("symlink? " + symlink);
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

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("mapper called");
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reducer called");
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        final String inputPath = args[0];
        final String outputPath = args[1];
        final String cacheFile = args[2];
        if (args.length == 4) {
            conf.set("innerFile", "/" + args[3]);
        }

        String symlink = cacheFile.substring(cacheFile.indexOf('#') + 1);
        conf.set("symlink", symlink);

        String containingFile = "";
        if (!symlink.equals("")) {
            containingFile = cacheFile.substring(cacheFile.lastIndexOf('/') + 1, cacheFile.indexOf('#') - 1);
        } else {
            containingFile = cacheFile.substring(cacheFile.lastIndexOf('/') + 1);
        }
        conf.set("containingFile", containingFile);

        System.out.println("Running word count");
        System.out.println("Input Path=" + inputPath);
        System.out.println("Output Path=" + outputPath);
        System.out.println("Setting up Job");

        conf.set("mapred.create.symlink", "yes");
        Job job = Job.getInstance(conf, "word count");
        // trying to add cache archive of form "zipfile.zip#myzip" fails
        // symlinks do not appear to work
        job.addCacheArchive(new Path(cacheFile).toUri());
        job.setJarByClass(WordCountExtraDep.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
