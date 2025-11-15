package ru.test;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class PgStress {

	private static final int NUM_THREADS = 10;
	private static final int NUM_TABLES = 5;
	private static final int NUM_RECORDS_PER_TABLE = 10000;

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
			for (int i = 1; i <= NUM_TABLES; i++) {
				String createTableSQL = String.format(
						"CREATE TABLE IF NOT EXISTS table_%d (" + "id SERIAL PRIMARY KEY, " + "name VARCHAR(100), "
								+ "email VARCHAR(100), " + "age INTEGER, " + "salary DECIMAL(10,2), "
								+ "created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " + "status VARCHAR(20)" + ")",
						i);

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(createTableSQL);
				}

				// Заполнение таблицы данными
				fillTableWithData(conn, i);
			}

			// Создание таблицы для связей (для JOIN операций)
			String createRelationsTable = "CREATE TABLE IF NOT EXISTS table_relations (" + "id SERIAL PRIMARY KEY, "
					+ "table_1_id INTEGER REFERENCES table_1(id), " + "table_2_id INTEGER REFERENCES table_2(id), "
					+ "table_3_id INTEGER REFERENCES table_3(id), " + "relation_type VARCHAR(50)" + ")";

			try (Statement stmt = conn.createStatement()) {
				stmt.execute(createRelationsTable);
			}

			// Заполнение таблицы связей
			fillRelationsTable(conn);
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

				// Случайное количество команд в транзакции (3-7)
				int numCommands = 3 + random.nextInt(5);

				System.out.printf("Worker %d: Начало транзакции с %d командами%n", workerId, numCommands);

				for (int i = 0; i < numCommands; i++) {
					executeRandomCommand(conn, workerId, i);
					// Небольшая задержка между командами
					Thread.sleep(100 + random.nextInt(200));
				}

				conn.commit();
				System.out.printf("Worker %d: Транзакция успешно завершена%n", workerId);

			} catch (Exception e) {
				System.err.printf("Worker %d: Ошибка в транзакции: %s%n", workerId, e.getMessage());
			}
		}

		private void executeRandomCommand(Connection conn, int workerId, int commandNum) throws SQLException {
			int commandType = random.nextInt(3); // 0=SELECT, 1=INSERT, 2=UPDATE

			switch (commandType) {
			case 0: // SELECT с JOIN
				executeSelectWithJoin(conn, workerId, commandNum);
				break;
			case 1: // INSERT
				executeInsert(conn, workerId, commandNum);
				break;
			case 2: // UPDATE
				executeUpdate(conn, workerId, commandNum);
				break;
			}
		}

		private void executeSelectWithJoin(Connection conn, int workerId, int commandNum) throws SQLException {
			// Выбираем случайный тип JOIN запроса
			int joinType = random.nextInt(3);
			String sql;

			switch (joinType) {
			case 0:
				sql = "SELECT t1.id, t1.name, t1.email, t2.age, t3.salary, tr.relation_type " + "FROM table_1 t1 "
						+ "JOIN table_2 t2 ON t1.id = t2.id " + "JOIN table_3 t3 ON t1.id = t3.id "
						+ "LEFT JOIN table_relations tr ON t1.id = tr.table_1_id " + "WHERE t1.status = 'ACTIVE' "
						+ "LIMIT 10";
				break;
			case 1:
				sql = "SELECT t1.name, t4.email, t5.salary, tr.relation_type " + "FROM table_1 t1 "
						+ "JOIN table_4 t4 ON t1.id = t4.id " + "JOIN table_5 t5 ON t1.id = t5.id "
						+ "JOIN table_relations tr ON t1.id = tr.table_1_id " + "WHERE t5.salary > 5000 "
						+ "ORDER BY t5.salary DESC " + "LIMIT 8";
				break;
			default:
				sql = "SELECT t2.id, t2.name, t3.email, t4.age, tr.relation_type " + "FROM table_2 t2 "
						+ "JOIN table_3 t3 ON t2.id = t3.id " + "JOIN table_4 t4 ON t2.id = t4.id "
						+ "LEFT JOIN table_relations tr ON t2.id = tr.table_2_id " + "WHERE t4.age BETWEEN 25 AND 45 "
						+ "LIMIT 12";
			}

			try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {

				int count = 0;
				while (rs.next()) {
					count++;
				}
				System.out.printf("Worker %d, Command %d: SELECT вернул %d строк%n", workerId, commandNum, count);
			}
		}

		private void executeInsert(Connection conn, int workerId, int commandNum) throws SQLException {
			int tableNum = 1 + random.nextInt(NUM_TABLES);
			String sql = String.format("INSERT INTO table_%d (name, email, age, salary, status) VALUES (?, ?, ?, ?, ?)",
					tableNum);

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, "NewUser_" + workerId + "_" + commandNum);
				pstmt.setString(2, "newuser_" + workerId + "_" + commandNum + "@example.com");
				pstmt.setInt(3, 18 + random.nextInt(50));
				pstmt.setDouble(4, 2000 + random.nextInt(8000));
				pstmt.setString(5, random.nextBoolean() ? "ACTIVE" : "INACTIVE");

				int rows = pstmt.executeUpdate();
				System.out.printf("Worker %d, Command %d: INSERT в table_%d, затронуто %d строк%n", workerId,
						commandNum, tableNum, rows);
			}
		}

		private void executeUpdate(Connection conn, int workerId, int commandNum) throws SQLException {
			int tableNum = 1 + random.nextInt(NUM_TABLES);
			String sql = String.format("UPDATE table_%d SET salary = salary * 1.1, status = ? WHERE id = ?", tableNum);

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, random.nextBoolean() ? "ACTIVE" : "INACTIVE");
				pstmt.setInt(2, 1 + random.nextInt(NUM_RECORDS_PER_TABLE));

				int rows = pstmt.executeUpdate();
				System.out.printf("Worker %d, Command %d: UPDATE в table_%d, затронуто %d строк%n", workerId,
						commandNum, tableNum, rows);
			}
		}
	}
}
