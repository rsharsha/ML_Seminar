package com.app.extra;

public class Sequence {
	private long id;
	private float[] values;
	private double[] distances;
	private int weight = 1;

	public double[] getDistances() {
		return distances;
	}

	public void setDistances(double[] distances) {
		this.distances = distances;
	}

	public Sequence() {

	}

	public Sequence(int id, float[] values) {
		super();
		this.id = id;
		this.values = values;
	}

	public Sequence(long id, String value) {
		super();
		this.id = id;
		String[] data = value.split(",");
		this.values = new float[data.length];
		for (int i = 0; i < data.length; i++) {
			this.values[i] = Float.parseFloat(data[i]);
		}
	}

	public Sequence(String wholeData) {
		String[] data = wholeData.split(";");
		this.id = Long.parseLong(data[0]);
		if (data.length > 1) {
			String[] data1 = data[1].split(",");
			this.values = new float[data1.length];
			for (int i = 0; i < data1.length; i++) {
				this.values[i] = Float.parseFloat(data1[i]);
			}
		}
		if (data.length > 2) {
			String[] data2 = data[2].split(",");
			this.distances = new double[data2.length];
			for (int i = 0; i < data2.length; i++) {
				this.distances[i] = Double.parseDouble(data2[i]);
			}
		}
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public float[] getValues() {
		return values;
	}

	public void setValues(float[] values) {
		this.values = values;
	}

	public String getSequenceWithIdAsString() {
		return id + ";" + getSequenceAsString();
	}

	public String getSequenceAsString() {
		String data = "";
		for (int i = 0; i < values.length; i++) {
			if (!"".equals(data)) {
				data += ",";
			}
			data += values[i];
		}
		return data;
	}

	public String toString() {
		String data = "";
		for (int i = 0; i < distances.length; i++) {
			if (!"".equals(data)) {
				data += ",";
			}
			data += distances[i];
		}
		return getSequenceWithIdAsString() + ";" + data;

	}

}
