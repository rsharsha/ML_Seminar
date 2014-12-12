package com.app.reducers;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.app.extra.Centroid;
import com.app.extra.DTW;
import com.app.extra.Sequence;
import com.app.main.HadoopMain;

public class KMeansReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	boolean isIterationRequired = false;
	int index = -1;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		isIterationRequired = false;
		index = Integer
				.parseInt(context.getConfiguration().getStrings("index")[0]);
	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashSet<Long> setData = new HashSet<Long>();
		Centroid[] centroids = null;
		FSDataOutputStream fsDataOutput = null;
		FSDataInputStream fsDataInput = null;
		BufferedOutputStream bufferedOutput = null;
		BufferedInputStream bufferedInput = null;
		String[] data = context.getConfiguration().getStrings(
				HadoopMain.CENTROIDS);
		centroids = new Centroid[data.length];
		for (int i = 0; i < data.length; i++) {
			centroids[i] = new Centroid(data[i]);
		}

		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path("/output/output" + index + "/o" + key);
		if (!fs.exists(path)) {
			fsDataOutput = fs.create(path, true);
		} else {
			fsDataOutput = fs.append(path);
		}
		bufferedOutput = new BufferedOutputStream(fsDataOutput);

		Path previousData = new Path("/output/output" + (index - 1) + "/o"
				+ key);
		if (!isIterationRequired && fs.exists(previousData)) {
			fsDataInput = fs.open(previousData);
			bufferedInput = new BufferedInputStream(fsDataInput);
			DataInputStream dis = new DataInputStream(bufferedInput);
			BufferedReader br = new BufferedReader(new InputStreamReader(dis));
			String input = null;
			while ((input = br.readLine()) != null) {
				String temp[] = input.split(",");
				setData.add(Long.parseLong(temp[1]));
			}
			br.close();
		}
		if ((index - 1) < 0) {
			isIterationRequired = true;
		}

		ArrayList<Sequence> sequences = new ArrayList<Sequence>();
		for (Text val : values) {
			Sequence temp = new Sequence(val.toString());
			sequences.add(temp);
			if (!isIterationRequired && !setData.contains(temp.getId())) {
				isIterationRequired = true;
			}
			bufferedOutput.write((key.get() + "," + temp.getId() + "\n")
					.getBytes());
			bufferedOutput.flush();
		}

		Centroid newCentroid = calculateCentroids(sequences);
		data[key.get()] = newCentroid.toString();
		context.write(key, new Text(newCentroid.toString()));
		bufferedOutput.close();
	}

	@Override
	public void cleanup(Context context) {
		if (isIterationRequired)
			context.getCounter(HadoopMain.CENTROIDS, "reIterate").increment(1);
	}

	private Centroid calculateCentroids(ArrayList<Sequence> sequences) {
		ArrayList<ArrayList<Double>> sDistance = new ArrayList<ArrayList<Double>>();
		for (int i = 0; i < sequences.size(); i++) {
			ArrayList<Double> distance = new ArrayList<Double>();
			for (int j = 0; j < sequences.size(); j++) {
				distance.add(getDistanceApprox(sequences.get(i),
						sequences.get(j)));
			}
			sDistance.add(distance);
		}
		while (sequences.size() > 1) {
			int index[] = getMinIndex(sDistance);
			Sequence s = CDTW(sequences.get(index[0]), sequences.get(index[1]));
			updateSDistance(sDistance, index);
			sequences.remove(index[0]);
			sequences.remove(index[1]);
			sequences.add(s);
		}
		return new Centroid(sequences.get(0).getValues());
	}

	private void updateSDistance(ArrayList<ArrayList<Double>> sDistance,
			int[] index) {
		ArrayList<Double> temp = new ArrayList<Double>();
		for (int i = 0; i < sDistance.size(); i++) {
			temp.add(Math.min(sDistance.get(index[0]).get(i),
					sDistance.get(index[1]).get(i)));
		}
		sDistance.add(temp);
		sDistance.remove(index[0]);
		sDistance.remove(index[1]);
		for (int i = 0; i < sDistance.size(); i++) {
			sDistance.get(i).remove(index[0]);
			sDistance.get(i).remove(index[1]);
			if (i < temp.size())
				sDistance.get(i).add(temp.get(i));
			else
				sDistance.get(i).add(0D);
		}

	}

	private int[] getMinIndex(ArrayList<ArrayList<Double>> sDistance) {
		int[] index = new int[2];
		index[0] = -1;
		index[1] = -1;
		double minValue = -1;
		for (int i = 0; i < sDistance.size(); i++) {
			double temp = Collections.min(sDistance.get(i));
			if (minValue == -1 || minValue > temp) {
				minValue = temp;
				index[0] = i;
				index[1] = sDistance.get(i).indexOf(
						Collections.min(sDistance.get(i)));
			}
		}
		return index;
	}

	private Sequence CDTW(Sequence sequence1, Sequence sequence2) {
		DTW distance = new DTW(sequence1.getValues(), sequence2.getValues());
		float[] values = new float[distance.getWarpingPath().length];
		for (int i = 0; i < values.length; i++) {
			int index[] = distance.getWarpingPath()[i];
			values[i] = ((sequence1.getValues()[index[0]] * sequence1
					.getWeight()) + (sequence2.getValues()[index[1]] * sequence2
					.getWeight()))
					/ (sequence1.getWeight() + sequence2.getWeight());
		}
		Sequence s = new Sequence(-1, values);
		s.setWeight(sequence1.getWeight() + sequence2.getWeight());
		return s;
	}

	private double getDistanceApprox(Sequence s1, Sequence s2) {
		double max = -1;
		for (int i = 0; i < s1.getDistances().length; i++) {
			double temp = Math.abs(s1.getDistances()[i] - s2.getDistances()[i]);
			if (max < temp) {
				max = temp;
			}
		}
		return max;
	}
}
