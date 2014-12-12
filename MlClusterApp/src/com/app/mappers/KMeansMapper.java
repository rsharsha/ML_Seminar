package com.app.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.app.extra.Centroid;
import com.app.extra.DTW;
import com.app.extra.Sequence;
import com.app.main.HadoopMain;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	Centroid[] centroids = null;
	String[] data = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		data = context.getConfiguration().getStrings(HadoopMain.CENTROIDS);
		centroids = new Centroid[data.length];
		for (int i = 0; i < data.length; i++) {
			centroids[i] = new Centroid(data[i]);
		}
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println(key);
		String[] data = value.toString().split(" ");
		Sequence sequence = new Sequence(Long.parseLong(data[0]), data[1]);
		DTW[] distances = new DTW[centroids.length];
		int minIndex = -1;
		double minDistance = -1;
		String output = "";
		for (int i = 0; i < centroids.length; i++) {
			if (!"".equals(output)) {
				output += ",";
			}
			distances[i] = new DTW(sequence.getValues(),
					centroids[i].getValues());
			if (minDistance == -1 || minDistance > distances[i].getDistance()) {
				minDistance = distances[i].getDistance();
				minIndex = i;
			}
			output += distances[i].getDistance();
		}

		output = sequence.getSequenceWithIdAsString() + ";" + output;
		one.set(minIndex);
		word.set(output);
		context.write(one, word);
	}
}
