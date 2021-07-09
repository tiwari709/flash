package in.edureka.occurence.assignment3;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class OccurrencesMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	public static final Log log = LogFactory.getLog(OccurrencesMapper.class);
	
	public enum PERSON_GROUP {
		DUNCAN, MALCOLM
	}
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString().trim();
		if (valueStr.length() > 0) {
			String values[] = valueStr.trim().split(" ");
			String alphaNumericStr = values[0].replaceAll("[^a-zA-Z0-9]", "");

			if ("DUNCAN".equalsIgnoreCase(alphaNumericStr)) {
				context.getCounter(PERSON_GROUP.DUNCAN).increment(1);
			}
			if ("MALCOLM".equalsIgnoreCase(alphaNumericStr)) {
				context.getCounter(PERSON_GROUP.MALCOLM).increment(1);
			}

		} else {
			System.out.println("value:" + valueStr);
		}
	}

}