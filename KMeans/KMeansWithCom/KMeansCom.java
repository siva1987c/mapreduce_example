import java.io.*;
import java.util.*;
import java.math.BigDecimal;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.iterative.*;
import org.apache.hadoop.mapred.JobConf;


public class KMeansCom extends Configured implements Tool{
	static String[] centerPoints = new String[8] ;
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void configure(JobConf conf) {
			try{
				FileSystem dfs = FileSystem.get(conf);	
				String location;
				if(conf.get("regionCloud")!=null){
                                        location = "topToRegionFile";
                                }
                                else{
                                        if(conf.getCurrentIteration()==0){
                                                location = "topToRegionFile";
                                        }
                                        else{
                                                location = conf.getOutputPath()+"/i"+(conf.getCurrentIteration()-1);
                                        }
                                }
				Path path = new Path(location);
				FileStatus[] files = dfs.listStatus(path);
				int j = 0; 
				for (int i = 0; i < files.length; i++){
					if (!files[i].isDir()) {
						FSDataInputStream is = dfs.open(files[i].getPath());
						String line = null;
						while ((line = is.readLine()) != null) {
							String fields[] = line.split("\t");
							centerPoints[j]=fields[1];
							j++;
						}
						is.close();
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			double[] row = parseStringToVector(line);
			double minDistance = Double.MAX_VALUE;
			String minID = "";
			for(int i = 0 ; i < centerPoints.length ; i++){
				if(centerPoints[i] != null){
					String id = Integer.toString(i);
					double[] point = parseStringToVector(centerPoints[i]);
					double currentDistance = distance(row, point);
					if (currentDistance < minDistance) {
						minDistance = currentDistance;
						minID = id;
					}
				}
			}
			output.collect(new Text(minID), new Text(line));
		}

		private double distance(double[] d1, double[] d2) {
			double distance = 0;
			int len = d1.length < d2.length ? d1.length : d2.length;

			for (int i = 1; i < len; i++) {
				distance += (d1[i] - d2[i]) * (d1[i] - d2[i]);
			}
			return Math.sqrt(distance);
		}
	}
	private static double[] parseStringToVector(String line) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();
			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			return row;
		} catch (Exception e) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int size = tokenizer.countTokens();
			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			return row;
		}
	}
	private static void accumulate(double[] sum, double[] array) {
		for (int i = 0; i < sum.length; i++)
			sum[i] += array[i];
	}
	
	public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


			double[] sum = {0,0,0,0,0,0,0,0,0,0};
			long count =0;

			while(values.hasNext()){
				String line = values.next().toString();
				if(line.contains("-")){
					String fields[] = line.split("-",2);
					double[] tmp = parseStringToVector(fields[1]);			
					accumulate(sum,tmp);
					count += Long.parseLong(fields[0]);
				}
				else{
					double[] tmp = parseStringToVector(line);
					accumulate(sum,tmp);
					count++;		
				}
			}
			
			String result = "";
			for (int i = 0; i < sum.length ; i++){
                                result += sum[i];
                                if(i+1 != sum.length)
                                        result += (",");
                        }
			result = count + "-" + result;
                        output.collect(key, new Text(result));

			/*
			double[] sum = {0,0,0,0,0,0,0,0,0,0};
			long count =0;
			while(values.hasNext()){
				double[] tmp = parseStringToVector(values.next().toString());
				accumulate(sum,tmp);
				count++;		
			}
			String result = "";
			for (int i = 0; i < sum.length ; i++){
				result += sum[i];
				if(i+1 != sum.length)
					result += (",");
			}
			result = count + "-" + result;
			output.collect(key, new Text(result));
			*/
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			double[] sum = {0,0,0,0,0,0,0,0,0,0};
			long count = 0;
			while(values.hasNext()){
				String line = values.next().toString();
				String fields[] = line.split("-",2);
				count += Long.parseLong(fields[0]);
				double[] tmp = parseStringToVector(fields[1]);				
				accumulate(sum,tmp);
			}
			
			String result =  "";
			for (int i = 0; i < sum.length ; i++) {
				sum[i] = sum[i] / count;
				sum[i] = round(sum[i], 2);
				result += (sum[i]);
				if(i+1 != sum.length)
					result += (",");
			}
			
			result.trim();
			output.collect(key, new Text(result));
		}

		double round(double v, int scale) {
			if (scale < 0) {
				throw new IllegalArgumentException(
						"The scale must be a positive integer or zero");
			}
			BigDecimal b = new BigDecimal(Double.toString(v));
			BigDecimal one = new BigDecimal("1");
			return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
		}

	}

	public int run(String[] args) throws Exception {


		JobConf job = new JobConf(getConf());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		JobConf jobf = new JobConf(getConf(),KMeansCom.class);
		jobf.setJobName("KMeansCom");
		//jobf.setNumReduceTasks(4);	
		jobf.setLoopInputOutput(KMeansLoopInputOutput.class);
		jobf.setInputPath(args[0]);
		jobf.setOutputPath(args[1]);
		jobf.setStepConf(0,job);
		jobf.setIterative(true);
		jobf.setNumIterations(3);
                //jobf.setLoopMapCacheFilter(KMeansLoopMapCacheFilter.class);
                //jobf.setLoopMapCacheSwitch(KMeansLoopMapCacheSwitch.class);

		JobClient.runJob(jobf);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KMeansCom(), args);
		System.exit(res);
	}
}

class KMeansLoopInputOutput implements LoopInputOutput {

	@Override
		public List<String> getLoopInputs(JobConf conf, int iteration, int step) {
			List<String> paths = new ArrayList<String>();
			paths.add(conf.getInputPath());
			return paths;
		}

	@Override
		public String getLoopOutputs(JobConf conf, int iteration, int step) {
			return (conf.getOutputPath() + "/i" + iteration);
		}

}


class KMeansLoopMapCacheFilter implements LoopMapCacheFilter {

	@Override
	public boolean isCache(Object key, Object value, int count) {
		return true;
	}

}

class KMeansLoopMapCacheSwitch implements LoopMapCacheSwitch {

	@Override
	public boolean isCacheRead(JobConf conf, int iteration, int step) {
		if (iteration > 0)
			return true;
		else
			return false;
	}

	@Override
	public boolean isCacheWritten(JobConf conf, int iteration, int step) {
		if (iteration == 0 && step == 0)
			return true;
		else
			return false;
	}
	
	@Override
	public Step getCacheStep(JobConf conf, int iteration, int step) {
		return new Step(0, 0);
	}

}
