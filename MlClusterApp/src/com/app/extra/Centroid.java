package com.app.extra;

public class Centroid {
	private float[] values;

	public float[] getValues() {
		return values;
	}

	public void setValues(float[] values) {
		this.values = values;
	}

	public Centroid(float[] values) {
		super();
		this.values = values;
	}

	public Centroid(String value) {
		String[] data = value.split(";");
		values = new float[data.length];
		for (int i = 0; i < values.length; i++) {
			this.values[i] = Float.parseFloat(data[i]);
		}
	}

	public String toString() {
		String data = "";
		for (int i = 0; i < values.length; i++) {
			if (!"".equals(data)) {
				data += ";";
			}
			data += values[i];
		}
		return data;
	}

}
