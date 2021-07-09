package in.edureka.maxsalary.assignment3;

import static java.lang.Integer.parseInt;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartmentSalaryMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	public static final Log log = LogFactory.getLog(DepartmentSalaryMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		if (key.get() != 0) {
			if (value.toString().length() > 0) {
				String arrEmpAttributes[] = value.toString().split(",");
				int deptNo = parseInt(arrEmpAttributes[7]);
				String empNo = arrEmpAttributes[0];
				String salary = arrEmpAttributes[5];
				context.write(new LongWritable(deptNo), new Text(empNo + "," + salary));
			} else {
				System.out.println("value:" + value.toString());
			}
		}
		System.out.println("value:" + value.toString());
	}

}