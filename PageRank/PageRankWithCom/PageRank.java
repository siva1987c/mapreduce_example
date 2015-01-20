import java.io.*;
import java.util.*;
import java.math.BigDecimal;
import java.text.NumberFormat; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.iterative.*;
import org.apache.hadoop.mapred.JobConf;

public class PageRank extends Configured implements Tool{

	private static HashMap<String, Float> nodeScore;
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void configure(JobConf conf) {
			nodeScore = new HashMap<String,Float>();
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
				for (int i = 0; i < files.length; i++) {
					if (!files[i].isDir()) {
						FSDataInputStream is = dfs.open(files[i].getPath());
						String line = null;
						while ((line = is.readLine()) != null) {
							String fields[] = line.split("\t",2);
							float tmp = Float.parseFloat(fields[1]);
							nodeScore.put(fields[0] , tmp);
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
			String lineSplit[] = line.split("\t");
			String srcNode = lineSplit[0];
			String dstNode[] = lineSplit[1].split(",");

			float emitScore; 
			float NumOfDst = (float)dstNode.length;
			float one = (float)1;
			NumberFormat nf = NumberFormat.getInstance();
			nf.setMaximumFractionDigits(4);
			
			String outValue;

			if(nodeScore.containsKey(srcNode)){
				emitScore = (float)nodeScore.get(srcNode)/NumOfDst;
				outValue = nf.format(emitScore);
			}
			else{
				emitScore = (float)1/NumOfDst;
				outValue = nf.format(emitScore);
			}
			for(int i = 0 ; i < dstNode.length ; i++){
				String k = dstNode[i]; 
				output.collect(new Text(k), new Text(outValue));

			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			float sum = 0;
			float tmp;
			while(values.hasNext()){
				tmp = Float.parseFloat(values.next().toString());
				sum = sum + tmp;
			}
			output.collect(key, new Text(Float.toString(sum)));
		}
	}

	public int run(String[] args) throws Exception {


		JobConf job = new JobConf(getConf());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		JobConf jobf = new JobConf(getConf(),PageRank.class);
		jobf.setJobName("PageRank");
		//jobf.setNumReduceTasks(8);	
		jobf.setLoopInputOutput(PageRankLoopInputOutput.class);
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
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}
}

class PageRankLoopInputOutput implements LoopInputOutput {

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
