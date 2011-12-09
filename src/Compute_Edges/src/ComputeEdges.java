

/*
MAP-REDUCE Section:

Round 1:
Map <Filename, Line> --> <ParticleID, globalID> 	//globalID = (snap<<32 + fofID)
Reduce: <particleID, edge> 		//edge is a pair of global IDs at adjacent timesteps

Round 2:
Map <particleID, edge> --> <edge, set(ParticleIDs)> //at this point set is a 1 member set
Reduce <edge, set(ParticleIDs)>

Round 3:
Map <edge, set(ParticleIDs)> --> <source, set([weight, sink])>
Reduce <node, set([weight, vertex])>
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ComputeEdges
{
	
	public static class R3FinalPrepMapper extends Mapper<PairWritable, LongArrayWritable, LongWritable, PairArrayWritable>
	{
		public void map(PairWritable edge, LongArrayWritable ids, Context context) throws IOException, InterruptedException
		{
			long weight = (long) ids.get().length;
			PairWritable pair = new PairWritable(weight, edge.sink);
			PairWritable[] out = new PairWritable[1];
			out[0] = pair;
			context.write(new LongWritable(edge.source), new PairArrayWritable(out));
		}
	}


	public static class R3FinalPrepReducer extends Reducer<LongWritable, PairArrayWritable, LongWritable, PairArrayWritable>
	{
		public void reduce(LongWritable source, Iterable<PairArrayWritable> values, Context context) throws IOException, InterruptedException
		{
			List<PairWritable> collectEdges = new ArrayList<PairWritable>();
			for(PairArrayWritable val: values)
			{
				collectEdges.addAll((Collection<? extends PairWritable>) Arrays.asList(val.get()));
			}
			PairWritable[] out = new PairWritable[collectEdges.size()];
			int i = 0;
			for(PairWritable p: collectEdges)
			{
				out[i] = p;
				i++;
			}
			context.write(source, new PairArrayWritable(out));
		}
	}

	//////////////////////////////////////////

	public static class R2EdgeWeightMapper extends Mapper<LongWritable, PairWritable, PairWritable, LongArrayWritable>
	{
		public void map(LongWritable partID, PairWritable edge, Context context) throws IOException, InterruptedException
		{
			LongWritable[] parts = new LongWritable[1];

			parts[0] = partID;
			context.write(edge, new LongArrayWritable(parts));
		}
	}

	public static class R2GetEdgeWeightReducer extends Reducer<PairWritable, LongArrayWritable, PairWritable, LongArrayWritable>
	{
		public void reduce(PairWritable key, Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException
		{
			List<LongWritable> collectParts = new LinkedList<LongWritable>();
			for(LongArrayWritable val: values)
			{
				LongWritable[] currentVal = (LongWritable[]) val.get();
				collectParts.addAll(Arrays.asList(currentVal));
			}

			int size = collectParts.size();
			LongWritable[] parts = new LongWritable[size];
			int i = 0;
			for(LongWritable l: collectParts)
			{
				parts[i] = l;
				i++;
			}
			context.write(key, new LongArrayWritable(parts));
		}
	}


	public static class R1ExplodeIDsMapper extends Mapper<LongWritable, Text, LongWritable, PairWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String [] input = line.split("\\s+");
			if(input.length > 0)
			{
				long globalID = Long.parseLong(input[0]);
				for(int i = 1; i < input.length; i++)
				{
					long partID = Long.parseLong(input[i]);
					context.write(new LongWritable(partID), new PairWritable(globalID, -1));
				}
			}
		}
	}


	public static class R1FindEdgeReducer extends Reducer<LongWritable, PairWritable, LongWritable, PairWritable>
	{
		public void reduce(LongWritable partID, Iterable<PairWritable> edges, Context context) throws IOException, InterruptedException
		{
			Map<Long, List<Long>> edgeFinder = new HashMap<Long, List<Long>>();
			//insert values into map
			for(PairWritable node: edges)
			{
				long snap = (node.source >>> 32L);
				if(edgeFinder.containsKey(snap))
				{
					//add global id to list of ids in this snapshot
					edgeFinder.get(snap).add(node.source);
				}
				else
				{
					List<Long> idList = new ArrayList<Long>();
					idList.add(node.source);
					edgeFinder.put(snap, idList);
				}
			}

			List<Long> currentIds = null;
			List<Long> nextIds = null;
			long maxSnap = Collections.max(edgeFinder.keySet());
			long minSnap = Collections.min(edgeFinder.keySet());
			for(long i = maxSnap; i > minSnap; i--)
			{
				currentIds = edgeFinder.get(i);
				nextIds = edgeFinder.get(i-1);
				if((currentIds != null) && (nextIds != null))
				{
					for(Long cur: currentIds)
					{
						for(Long next: nextIds)
						{
							context.write(partID, new PairWritable(cur, next));
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		

		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		ControlledJob job1 = new ControlledJob(conf1);
		Job jc1 = job1.getJob();
		jc1.setJarByClass(ComputeEdges.class);
		jc1.setMapperClass(R1ExplodeIDsMapper.class);
		jc1.setReducerClass(R1FindEdgeReducer.class);
		jc1.setOutputKeyClass(LongWritable.class);
		jc1.setOutputValueClass(PairWritable.class);
		FileInputFormat.addInputPath(jc1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(jc1, new Path("/tmp/comp_edgesR1/"));
		


		Configuration conf2 = new Configuration();
		ControlledJob jc2 = new ControlledJob(conf2);
		Job job2 = jc2.getJob();
		job2.setJarByClass(ComputeEdges.class);
		job2.setMapperClass(R2EdgeWeightMapper.class);
		job2.setReducerClass(R2GetEdgeWeightReducer.class);
		job2.setOutputKeyClass(PairWritable.class);
		job2.setOutputValueClass(LongArrayWritable.class);
		FileInputFormat.addInputPath(job2,new Path("/tmp/comp_edgesR1/"));
		FileOutputFormat.setOutputPath(job2, new Path("/tmp/comp_edgesR2/"));
		jc2.addDependingJob(job1);
		
		Configuration conf3 = new Configuration();
		ControlledJob jc3 = new ControlledJob(conf3);
		Job job3 = jc3.getJob();
		job3.setJarByClass(ComputeEdges.class);
		job3.setMapperClass(R3FinalPrepMapper.class);
		job3.setReducerClass(R3FinalPrepReducer.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(PairArrayWritable.class);
		FileInputFormat.addInputPath(job3,new Path("/tmp/comp_edgesR2/"));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));
		jc3.addDependingJob(jc2);
		jc3.addDependingJob(job1);

		JobControl jobControl = new JobControl("Controller");
		jobControl.addJob(job1);
		jobControl.addJob(jc2);
		jobControl.addJob(jc3);
		jobControl.run();
	}
}















