package ru.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConfig {
	
	public static final String URL = "jdbc:postgresql://localhost:5432/testdb";
	public static final String PASSWORD = "password";
	public static final String USER = "mizhka";

	public static Connection getConnection() throws SQLException {
		String url = URL;
		Properties props = new Properties();
		props.setProperty("user", USER);
		props.setProperty("password", PASSWORD);
		props.setProperty("ssl", "false");
		props.setProperty("tcpKeepAlive", "true");
		props.setProperty("reWriteBatchedInserts", "true");

		return DriverManager.getConnection(url, props);
	}

	// Метод для создания базы данных если не существует
	public static void createDatabaseIfNotExists() {
		try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", USER, PASSWORD)) 
		{
			try (var stmt = conn.createStatement()) {
				stmt.execute("CREATE DATABASE testdb");
				System.out.println("База данных testdb создана");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42P04")) {
					System.out.println("База данных testdb уже существует");
				} else {
					throw e;
				}
			}
		} catch (SQLException e) {
			System.err.println("Ошибка при создании базы данных: " + e.getMessage());
		}
	}
}