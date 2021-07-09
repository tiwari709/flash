package in.edureka.maxsalary.assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class DepartmentSalaryReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<Integer> empNos = new ArrayList<Integer>();
		Double maxSal = 0.0;

		for (Text emp : values) {
			String[] split = emp.toString().split(",");
			int empNo = Integer.parseInt(split[0]);
			double sal = Double.parseDouble(split[1]);

			if (maxSal < sal) {
				maxSal = sal;
				empNos = new ArrayList<Integer>();
				empNos.add(empNo);
			} else if (maxSal == sal) {
				empNos.add(empNo);
			}
		}
		context.write(new Text("Dept=" + key),
				new Text(" max(salary)=" + maxSal + " EmpNo=" + StringUtils.join("|", empNos)));
	}

}