package com.app.main;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.app.mappers.KMeansMapper;
import com.app.reducers.KMeansReducer;

public class HadoopMain {

	private static final transient Logger LOG = LoggerFactory
			.getLogger(HadoopMain.class);
	public static final String CENTROIDS = "centroids";
	public static final int CLUSTER_SIZE = 3;
	public static final int DATA_SIZE = 100;
	public static final int ITERATIONS = 50;

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/output";

		/*
		 * FileOutputFormat wants to create the output directory itself. If it
		 * exists, delete it:
		 */
		deleteFolder(conf, outputPath);

		int i = 0, temp = 0;
		Random randomGenerator = new Random();
		int[] centers = new int[CLUSTER_SIZE];
		String[] centroids = new String[CLUSTER_SIZE];
		for (int j = 0; j < CLUSTER_SIZE; j++) {
			centers[j] = randomGenerator.nextInt(DATA_SIZE);
			System.out.println(centers[j]);
		}
		java.util.Arrays.sort(centers);
		String data1 = null;

		String dataInputPath = inputPath + "/data.txt";
		FSDataInputStream fsDataInput = null;
		Path tempPath = new Path(dataInputPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(tempPath)) {
			fsDataInput = fs.open(tempPath);
			BufferedInputStream bufferedInput = new BufferedInputStream(
					fsDataInput);
			DataInputStream dis = new DataInputStream(bufferedInput);
			BufferedReader br1 = new BufferedReader(new InputStreamReader(dis));
			while ((data1 = br1.readLine()) != null) {
				String[] data = data1.split(" ");
				if (i == centers[temp]) {
					centroids[temp] = data[1];
					centroids[temp] = centroids[temp].replace(',', ';');
					temp++;
					if (temp == CLUSTER_SIZE) {
						break;
					}
				}
				i++;
			}
			br1.close();
		} else {
			throw new Exception("No input filepath");
		}

		int index = 0;
		boolean iterate = true;
		conf.set("dfs.replication", "1");
		while (iterate) {
			if (index > 0) {
				String tempPreviousoutputPath = outputPath + "/output"
						+ (index - 1) + "/part-r-00000";
				fsDataInput = null;
				tempPath = new Path(tempPreviousoutputPath);
				fs = FileSystem.get(conf);
				if (fs.exists(tempPath)) {
					fsDataInput = fs.open(tempPath);
					BufferedInputStream bufferedInput = new BufferedInputStream(
							fsDataInput);
					DataInputStream dis = new DataInputStream(bufferedInput);
					BufferedReader br1 = new BufferedReader(
							new InputStreamReader(dis));
					String input = null;
					String newCentroidsData = "";
					while ((input = br1.readLine()) != null) {
						if (!"".equals(newCentroidsData)) {
							newCentroidsData += ",";
						}
						newCentroidsData += input.split("\t")[1];
					}
					conf.setStrings(CENTROIDS, newCentroidsData);
					br1.close();
				}
			} else {
				String newCentroidsData = "";
				for (int id = 0; id < centroids.length; id++) {
					if (!"".equals(newCentroidsData)) {
						newCentroidsData += ",";
					}
					newCentroidsData += centroids[id];
				}
				conf.setStrings(CENTROIDS, newCentroidsData);
			}

			conf.setStrings("index", "" + index);
			String tempoutputPath = outputPath + "/output" + index;
			Job job = Job.getInstance(conf);

			job.setJarByClass(HadoopMain.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(tempoutputPath));

			if (job.waitForCompletion(true)) {
				String[] tempData = conf.getStrings(CENTROIDS);
				for (String t : tempData) {
					System.out.println(t);
				}
				// System.exit(0);
			}
			long counterValue = job.getCounters().getGroup(CENTROIDS)
					.findCounter("reIterate").getValue();
			System.out.println(counterValue + "is iteration");
			if (counterValue == 0) {
				iterate = false;
			}
			index++;
			if (index == ITERATIONS) {
				break;
			}
		}

	}

	private static void deleteFolder(Configuration conf, String folderPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

}
