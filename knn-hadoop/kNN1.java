import java.io.FileWriter;
import java.util.HashMap;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import java.util.Map;
import java.util.ArrayList;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import java.io.File;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Job;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import java.io.BufferedWriter;
import java.util.List;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.mapreduce.Reducer;
//import java.lang.NullPointerExcpeption;
import java.net.URI;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;	
import org.apache.hadoop.io.WritableComparable;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import java.io.DataInput;

public class kNN1{
	static int K = 11;	//will this work?

	public static class knnMap extends Mapper<Object,Text,IntWritable,Text>{
		String s;
		static IntWritable result;
		public static ArrayList<String> str_array = new ArrayList<String>();
		public static String string2;

		ArrayList<Integer> list = new ArrayList<Integer>();
		ArrayList<Integer> rlist = new ArrayList<Integer>();


		protected void setup(Context cInstance) throws IOException,InterruptedException{

			string2 = cInstance.getConfiguration().get("data");
			System.out.println(string2);

			StringTokenizer strTok = new StringTokenizer(string2,"\n");

			int j = 0;
			while(strTok.hasMoreTokens()){
				str_array.add(j,strTok.nextToken());	
				j++;
			}

			// for(int j = 0;j<30;j++){
			// 	str_array.add(j,"55 0 81 0 -6 11 25 88 64 4");
			// }

		}

		public void map(Object key, Text value,Context cInstance) throws IOException, InterruptedException{
			//System.out.println(str_array.get(10));
			for(int j = 0;j<str_array.size();j++){
				String line = value.toString();
				StringTokenizer strTok = new StringTokenizer(line," ");

				String linea1 = str_array.get(j);
				StringTokenizer sta1 = new StringTokenizer(linea1," ");

				for(int i = 0;i<10;i++){
					list.add(i,Integer.parseInt(sta1.nextToken()));
					rlist.add(i,Integer.parseInt(strTok.nextToken()));
				}

				int dist1 = 0;
				for(int i = 0;i<9;i++){
					dist1+= Math.pow((list.get(i)-rlist.get(i)),2);
				}


				result = new IntWritable(j);

				s = Integer.toString(dist1) + " " + Integer.toString(rlist.get(9));
				Text t = new Text(s);
				cInstance.write(result,t);

			} 
		}

	}

	public static class kNN_Reduce extends Reducer<IntWritable,Text,NullWritable,Text>{
		int num = 0;
		int correct = 0;

		public static String string2;
		TreeMap<Integer,Integer> objectMap = new TreeMap<Integer,Integer>();
		ArrayList<Integer> list = new ArrayList<Integer>();
		public static ArrayList<String> str_array = new ArrayList<String>();
		

		
		protected void setup(Context cInstance) throws IOException,InterruptedException{
			string2 = cInstance.getConfiguration().get("data");
			System.out.println(string2);

			StringTokenizer strTok = new StringTokenizer(string2,"\n");

			int j = 0;
			while(strTok.hasMoreTokens()){
				str_array.add(j,strTok.nextToken());
				j++;
			}

			num = 0;
			correct = 0;
			
		}

		public void reduce(IntWritable key, Iterable<Text> values, Context cInstance) throws IOException, InterruptedException
		{

			num++;
			for (Text val : values)
			{
				String str = val.toString();
				StringTokenizer st3 = new StringTokenizer(str," ");
				int dist = Integer.parseInt(st3.nextToken());
				int cls = Integer.parseInt(st3.nextToken());
				
				objectMap.put(dist, cls);
				if (objectMap.size() > K)
				{
					objectMap.remove(objectMap.lastKey());
				}
			}	


				ArrayList<Integer> objL = new ArrayList<Integer>(objectMap.values());

				Map<Integer, Integer> occurence = new HashMap<Integer, Integer>();
			    
			    for(int i=0; i< objL.size(); i++)
			    {  
			        Integer num_times = occurence.get(objL.get(i));
			        if(num_times == null)
			        {
			            occurence.put(objL.get(i), 1);
			        } else
			        {
			            occurence.put(objL.get(i), num_times+1);
			        }
			    }
			    
			    Integer maxCls = null;
			    int maxOcc = -1;
			    for(Map.Entry<Integer, Integer> occ: occurence.entrySet())
			    {
			        if(maxOcc < occ.getValue())
			        {
			            maxCls = occ.getKey();
			            maxOcc = occ.getValue();
					}
				}
				
				String line1 = str_array.get(key.get());

				StringTokenizer sta1 = new StringTokenizer(line1," ");

				for(int i = 0;i<10;i++){
					list.add(i,Integer.parseInt(sta1.nextToken()));
				}

				if(list.get(9)==maxCls){
					correct++;
				}

			cInstance.write(NullWritable.get(), new Text(Integer.toString(maxCls)+"  "+Integer.toString(key.get()))); 
			System.out.println(maxCls);
		}

		
		protected void cleanup(Context cInstance) throws IOException, InterruptedException{

			cInstance.write(NullWritable.get(),new Text("percentage accuracy: "+ Double.toString(((correct*1.0)/num)*100)));
		}


	}

	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException
	{
		//kNN1 abc = new kNN1();

		//newConfig.set("name",args[2]);

		Configuration newConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(newConfig);
		BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[2]))));
		// BufferedReader br=new BufferedReader(new InputStreamReader(new Path(cInstance.getConfiguration().get("name"))));

		String line=null;
		String lineall = "";
		int numoffeatures=0;
		int i = 0;
		while((line=br.readLine())!=null)
		{
			System.out.println(line);
			/*String[] feature=line.split(" ");
			numoffeatures=feature.length;
			for(int j=0;j<numoffeatures;j++)
				newConfig.setInt(""+j,Integer.parseInt(feature[j]));*/
			//knnMap.str_array.add(i,line);
			//i++;
			lineall+=line+"\n";
		}

		//System.out.println(lineall);
		newConfig.set("data",lineall);

		// knnMap.str_array.add(0,"55 0 81 0 -6 11 25 88 64 4");
		// knnMap.str_array.add(1,"56 0 96 0 52 -4 40 44 4 4");
		// knnMap.str_array.add(2,"50 -1 89 -7 50 0 39 40 2 1");
		// knnMap.str_array.add(3,"53 9 79 0 42 -2 25 37 12 4");

		br.close();
		hdfs.close();


		Job jobInstance = Job.getInstance(newConfig, "K nearest neighbours");	//
		
		jobInstance.setJarByClass(kNN1.class);

		
		jobInstance.setMapperClass(knnMap.class);
		jobInstance.setMapOutputKeyClass(IntWritable.class);
		jobInstance.setMapOutputValueClass(Text.class);
		jobInstance.setReducerClass(kNN_Reduce.class);
		jobInstance.setNumReduceTasks(1);
		jobInstance.setOutputKeyClass(NullWritable.class);
		jobInstance.setOutputValueClass(Text.class);
		

		
		FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobInstance, new Path(args[1]));

		

		//tmpDataFile.delete();

		jobInstance.waitForCompletion(true);
		
		System.exit(0);
	}	
}
