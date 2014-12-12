package com.app.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * This class retrieves data from database and writes the data into file.
 * This file is uploaded to hdfs for clustering.
 */
public class Main {

	public static void main(String[] args) {
		Statement stmt = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");

			connection = DriverManager
					.getConnection(
							"jdbc:postgresql://ubdatasciencedb.cm6g0idlhyvv.us-west-2.rds.amazonaws.com:5432/datasciencedb",
							"readonlyuser", "ubReadonlyUser");

			String query = "select idperson, resultdate, resultvaluenum from cdr_lab_result where valuename = 'LR_CR' and resultvaluenum is not null and resultdate between '2000-01-01' and '2014-01-01' group by idperson, resultdate, resultvaluenum order by idperson, resultdate";
			File file = new File("data.txt");

			if (!file.exists()) {
				file.createNewFile();
			}
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			String output = "";
			int personId = -1;
			while (rs.next() || rs.isAfterLast()) {
				int tempPersonId = rs.isAfterLast() ? -1 : rs.getInt(1);
				if ((personId != tempPersonId && personId != -1)
						|| rs.isAfterLast()) {
					output = personId + " " + output + "\n";
					bw.write(output);
					output = "";
					personId = -1;
					if (rs.isAfterLast())
						break;
				}
				personId = tempPersonId;
				if (!"".equals(output)) {
					output += ",";
				}
				output += rs.getDouble(3);
			}
			bw.flush();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (bw != null) {
					bw.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
