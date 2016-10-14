package org.zhanggw.mysql.select;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class MysqlSelect {

	private static String[] sqlClause = {
		 "select * from dw_biz_invest_statements_qianwan where create_day < '20160717'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160717' and create_day < '20160728'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160728' and create_day < '20160806'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160806' and create_day < '20160816'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160816' and create_day < '20160825'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160825' and create_day < '20160907'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160907' and create_day < '20160917'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160917' and create_day < '20160927'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20160927' and create_day < '20161006'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20161006' and create_day < '20161016'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20161016' and create_day < '20161024'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20161024' and create_day < '20161031'", 
		 "select * from dw_biz_invest_statements_qianwan where create_day >= '20161031'"
		 };
	
	public static void main(String[] args) {
		String urlInsert = "jdbc:mysql://172.30.249.156:3306/test"; 
		String username = "canal";
		String password = "canal";
		
		int num = sqlClause.length;
		Thread[] threads = new Thread[num];
		
		long startTotal = System.currentTimeMillis();
		
		for(int t = 0; t < num; t++) {
			try {
				Connection connection = DriverManager.getConnection( urlInsert, username, password);
				threads[(int) t] = new Thread(new WorkRunnable(t, connection));
				threads[(int) t].start();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		for(Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("total time " + (System.currentTimeMillis() - startTotal) / 1000);
	}

	static class WorkRunnable implements Runnable {

		private int index;
		private Connection conn;
		
		WorkRunnable(int index, Connection conn) {
			this.index = index;
			this.conn = conn;
		}
		
		@Override
		public void run() {
			long startsingle = System.currentTimeMillis();
			String curSql = sqlClause[index];
			try {
				boolean success = conn.createStatement().execute(curSql);
				System.out.println("thread:" + index + " time:" + (System.currentTimeMillis() - startsingle) / 1000);
			} catch (SQLException e) {
				System.out.println("myself error");
				e.printStackTrace();
			}
		}
		
	}
}
