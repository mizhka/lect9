package ru.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PgInsert {

	private static final int NUM_THREADS = 2;
	private static final int NUM_TABLES = NUM_THREADS;
	private static final int NUM_RECORDS_PER_TABLE = 10000;
	private static final int NUM_ITERS = 100000;

	private static final Random random = new Random();

	public static void main(String[] args) {
		try {
			DatabaseConfig.createDatabaseIfNotExists();
			
			// Создание таблиц и заполнение данными
			createTablesAndData();

			// Создание и запуск потоков
			ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
			List<Future<?>> futures = new ArrayList<>();

			for (int i = 0; i < NUM_THREADS; i++) {
				futures.add(executor.submit(new DatabaseWorker(i)));
				//futures.add(executor.submit(new DatabaseWorkerBatch(i)));
			}

			// Ожидание завершения всех потоков
			for (Future<?> future : futures) {
				future.get();
			}

			executor.shutdown();
			System.out.println("Все транзакции выполнены успешно!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createTablesAndData() throws SQLException {
		try (Connection conn = DatabaseConfig.getConnection()) {

			// Создание таблиц
			for (int i = 0; i < NUM_TABLES; i++) {
				String createTableSQL = String.format(
						"CREATE TABLE table_%d (" + "id SERIAL PRIMARY KEY, " + "name VARCHAR(100), "
								+ "email VARCHAR(100), " + "age INTEGER, " + "salary DECIMAL(10,2), "
								+ "created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " + "status VARCHAR(20)" + ")",
						i);

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSQL);
					// Заполнение таблицы данными
					fillTableWithData(conn, i);
				} catch (SQLException e) {
					if (e.getSQLState().equals("42P07")) {
						System.out.println("Таблица уже существует");
					} else {
						throw e;
					}
				}
			}
		}
	}

	private static void fillTableWithData(Connection conn, int tableNum) throws SQLException {
		String insertSQL = String
				.format("INSERT INTO table_%d (name, email, age, salary, status) VALUES (?, ?, ?, ?, ?)", tableNum);

		try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
			for (int i = 0; i < NUM_RECORDS_PER_TABLE; i++) {
				pstmt.setString(1, "User_" + tableNum + "_" + i);
				pstmt.setString(2, "user" + tableNum + "_" + i + "@example.com");
				pstmt.setInt(3, 20 + random.nextInt(50)); // возраст от 20 до 70
				pstmt.setDouble(4, 1000 + random.nextInt(9000)); // зарплата от 1000 до 10000
				pstmt.setString(5, random.nextBoolean() ? "ACTIVE" : "INACTIVE");
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}
	}

	private static void fillRelationsTable(Connection conn) throws SQLException {
		String insertSQL = "INSERT INTO table_relations (table_1_id, table_2_id, table_3_id, relation_type) VALUES (?, ?, ?, ?)";

		try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
			for (int i = 0; i < 50; i++) {
				pstmt.setInt(1, 1 + random.nextInt(NUM_RECORDS_PER_TABLE));
				pstmt.setInt(2, 1 + random.nextInt(NUM_RECORDS_PER_TABLE));
				pstmt.setInt(3, 1 + random.nextInt(NUM_RECORDS_PER_TABLE));
				pstmt.setString(4, "TYPE_" + random.nextInt(5));
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}
	}

	// Класс-рабочий для выполнения транзакций в отдельном потоке
	static class DatabaseWorker implements Runnable {
		private final int workerId;

		public DatabaseWorker(int workerId) {
			this.workerId = workerId;
		}

		@Override
		public void run() {
			try (Connection conn = DatabaseConfig.getConnection()) {
				// Устанавливаем уровень изоляции
				conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				conn.setAutoCommit(false);

				int commands = NUM_ITERS;
				System.out.printf("Worker %d: Начало транзакции с %d командами%n", workerId, commands);
				long s = System.nanoTime();
				for (int i = 0; i < commands; i++) {
					executeInsert(conn, workerId, i);
				}

				conn.commit();
				System.out.printf("Worker %d: Транзакция успешно завершена: %f %n", workerId, (double)(System.nanoTime() - s) / (1000 * 1000 * 1000));

			} catch (Exception e) {
				System.err.printf("Worker %d: Ошибка в транзакции: %s%n", workerId, e.getMessage());
			}
		}

		private void executeInsert(Connection conn, int workerId, int seq) throws SQLException {
			int tableNum = workerId;
			String sql = String.format("INSERT INTO table_%d (name, email, age, salary, status) VALUES (?, ?, ?, ?, ?)",
					tableNum);

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, "NewUser_" + workerId + "_" + seq);
				pstmt.setString(2, "newuser_" + workerId + "_" + seq + "@example.com");
				pstmt.setInt(3, 18 + random.nextInt(50));
				pstmt.setDouble(4, 2000 + random.nextInt(8000));
				pstmt.setString(5, random.nextBoolean() ? "ACTIVE" : "INACTIVE");

				pstmt.executeUpdate();
			}
		}
	}
	
	// Класс-рабочий для выполнения транзакций в отдельном потоке
		static class DatabaseWorkerBatch implements Runnable {
			private final int workerId;
			private static final int BATCH_SIZE = 50;
			public DatabaseWorkerBatch(int workerId) {
				this.workerId = workerId;
			}

			@Override
			public void run() {
				try (Connection conn = DatabaseConfig.getConnection()) {
					// Устанавливаем уровень изоляции
					conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					conn.setAutoCommit(false);

					int commands = NUM_ITERS;
					System.out.printf("Worker %d: Начало транзакции с %d командами%n", workerId, commands);
					long s = System.nanoTime();
					for (int i = 0; i < commands / BATCH_SIZE; i++) {
						executeInsert(conn, workerId, i);
					}

					conn.commit();
					System.out.printf("Worker %d: Транзакция успешно завершена: %f %n", workerId, (double)(System.nanoTime() - s) / (1000 * 1000 * 1000));

				} catch (Exception e) {
					System.err.printf("Worker %d: Ошибка в транзакции: %s%n", workerId, e.getMessage());
				}
			}

			private void executeInsert(Connection conn, int workerId, int seq) throws SQLException {
				int tableNum = workerId;
				String sql = String.format("INSERT INTO table_%d (name, email, age, salary, status) VALUES (?, ?, ?, ?, ?)",
						tableNum);

				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					
					for (int i = 0; i < BATCH_SIZE; i++) {
						pstmt.setString(1, "NewUser_" + workerId + "_" + (seq + i));
						pstmt.setString(2, "newuser_" + workerId + "_" + (seq + i) + "@example.com");
						pstmt.setInt(3, 18 + random.nextInt(50));
						pstmt.setDouble(4, 2000 + random.nextInt(8000));
						pstmt.setString(5, random.nextBoolean() ? "ACTIVE" : "INACTIVE");
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}
		}
	
}
