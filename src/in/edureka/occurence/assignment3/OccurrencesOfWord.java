package in.edureka.occurence.assignment3;

import static java.util.Arrays.asList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import in.edureka.occurence.assignment3.OccurrencesMapper.PERSON_GROUP;




public class OccurrencesOfWord extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int runStatus = ToolRunner.run(new Configuration(), new OccurrencesOfWord(), args);
		System.exit(runStatus);
	}
	

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://nameservice1");
		FileSystem fileSystem = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "OccurrencesOfWord");
		job.setJobName("OccurrencesOfWord");
		job.setJarByClass(getClass());
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setMapperClass(OccurrencesMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);

		FileStatus[] sourceListFiles = fileSystem
				.listStatus(new Path("hdfs:///bigdatapgp/common_folder/assignment3/frequence/"));

		for (FileStatus fileStatus : asList(sourceListFiles)) {
			Path sourceFilePath = fileStatus.getPath();
			FileInputFormat.setInputPaths(job, sourceFilePath);
		}
		Path targetPath = new Path(args[0]);
		if (fileSystem.exists(targetPath)) {
			fileSystem.delete(targetPath, true);
		}
		FileOutputFormat.setOutputPath(job, targetPath);

		int code = job.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			Counters counters = job.getCounters();
			Counter c1 = counters.findCounter(PERSON_GROUP.DUNCAN);
			System.out.println(c1.getDisplayName() + " : " + c1.getValue());
			c1 = counters.findCounter(PERSON_GROUP.MALCOLM);
			System.out.println(c1.getDisplayName() + " : " + c1.getValue());
		}
		return code;
	}

}
