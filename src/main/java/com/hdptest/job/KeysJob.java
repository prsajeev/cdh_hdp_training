package com.hdptest.job;


import java.io.IOException;  
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 

import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

import com.hdptest.mapper.KeysMapper;
import com.hdptest.reducer.KeysReducer;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class KeysJob 
{
	

		public static void main(String[] args) throws IOException,  InterruptedException, ClassNotFoundException 
		{           
			Path inputPath = new Path(args[0]); 
			Path keyFilePath = new Path(args[1]);
			Path outputPath = new Path(args[2]); 
			
			String inputFileName = inputPath.getName();
			String keyFileName = keyFilePath.getName();
			
			
			
			// Create configuration         
			Configuration conf = new Configuration(true);   
			
			
			conf.set("inputFileName", inputFileName);
			conf.set("keyFileName", keyFileName);
			
			conf.set("keyFilePath",args[1]);
			conf.set("outputPath",args[2]);
			// Create job         
			Job job = new Job(conf, "KeyGen");         
			job.setJarByClass(KeysJob.class);           
			// Setup MapReduce         
			job.setMapperClass(KeysMapper.class);         
			job.setReducerClass(KeysReducer.class);         
			job.setNumReduceTasks(1);           
			// Specify key / value         
			job.setOutputKeyClass(Text.class);         
			job.setOutputValueClass(Text.class);           
			// Input         
			FileInputFormat.addInputPath(job, inputPath);  
			FileInputFormat.addInputPath(job, keyFilePath);  
			
			job.setInputFormatClass(TextInputFormat.class);           
			         
			         
			
			
			
			
			// Delete output if exists         
			FileSystem hdfs = FileSystem.get(conf);         
			if (hdfs.exists(outputPath))             
				hdfs.delete(outputPath, true);           
				// Execute job         
			int code = job.waitForCompletion(true) ? 0 : 1;         
			System.exit(code);       
				
		}


	}

