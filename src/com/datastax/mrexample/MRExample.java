package com.datastax.mrexample;

import java.io.IOException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRExample extends Configured implements Tool
{
	//create global variables
	private static final String KEYSPACE = "test";
    private static final String COLUMN_FAMILY = "mrexample";
	private static final String TABLE_NAME = KEYSPACE + "." + COLUMN_FAMILY;
	private static final String NODES = "192.168.56.11";
	
    public static void main(String[] args) throws Exception
    {
    	// Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new Configuration(), new MRExample(), args);
        System.exit(res);
    }

    public static class ExampleMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
    	// create and reuse cluster, sessions, and preparedstatement 
    	// these are shared among each map instance (i.e. per split)
    	private Cluster cluster;
    	private Session session;
    	private static final String INSERT_CQL = "INSERT INTO " + TABLE_NAME
    			+ "( key, value) VALUES (?, ?)";
    	private PreparedStatement insertStmt;
    	
    	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
    			throws IOException, InterruptedException
    	{
    		// setup shared objects
    		cluster = Cluster.builder().addContactPoints(NODES).build();
    		session = cluster.connect();
    		insertStmt = session.prepare(INSERT_CQL);
    	}
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {   
    		BoundStatement bs = insertStmt.bind();
    		
    		//for this simple example we will simply pass in the key and value from the mapper
    		bs.setLong(0, key.get());
			bs.setString(1, value.toString());
			session.executeAsync(bs);
        }
    	
    	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
    			throws IOException, InterruptedException
    	{
    		//tear down shared objects
    		session.close();
    		cluster.close();
    	}
    }
 
    public int run(String[] args) throws Exception  {
    	if (args.length != 2)
    	{
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }
    	
    	Job job = Job.getInstance(super.getConf());
    	job.setJarByClass(MRExample.class);
    	job.setMapperClass(ExampleMapper.class);
    	//make this job map only
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(MRExample.class);
        boolean success = job.waitForCompletion(true);
        
        return success ? 0 : 1;
        }

}
