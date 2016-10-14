package org.zhanggw.mysql.insert;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat; 

public class MysqlInsertNew {
	
	public static int[] randomGet000(int top, int count) {
		int[] ret = new int[count];
		
		int num = 0;
		while(num < count) {
			int candidate = (int)(Math.random() * top);
			if(Arrays.binarySearch(ret, candidate) < 0 )
				ret[num++] = candidate;	
		}
		
		return ret;
	}
	
	public static String string2MD5(String inStr){  
		MessageDigest md5 = null;  
		try{  
			md5 = MessageDigest.getInstance("MD5");  
		}catch (Exception e){  
			System.out.println(e.toString());  
			e.printStackTrace();  
			return "";  
		}  
		char[] charArray = inStr.toCharArray();  
		byte[] byteArray = new byte[charArray.length];  
		
		for (int i = 0; i < charArray.length; i++)  
			byteArray[i] = (byte) charArray[i];  
		byte[] md5Bytes = md5.digest(byteArray);  
		StringBuffer hexValue = new StringBuffer();  
		for (int i = 0; i < md5Bytes.length; i++){  
			int val = ((int) md5Bytes[i]) & 0xff;  
			if (val < 16)  
				hexValue.append("0");  
			hexValue.append(Integer.toHexString(val));  
		}  
		return hexValue.toString();  

	}
	
	public static void main(String[] args) throws ParseException {
		Connection conn = null;  
		Statement statement = null;  
		String url = "jdbc:mysql://172.30.249.156:3306/DW"; 
		String urlInsert = "jdbc:mysql://172.30.249.156:3306/test"; 
		String username = "canal";
		String password = "canal";
		
		try {   
            Class.forName("com.mysql.jdbc.Driver" );   
            conn = DriverManager.getConnection( url,username, password);
            }  
		catch ( ClassNotFoundException cnfex ) {  
            System.err.println(  
            "装载 JDBC/ODBC 驱动程序失败。" );  
            cnfex.printStackTrace();   
        }   
		
		catch ( SQLException sqlex ) {  
            System.err.println( "无法连接数据库" );  
            sqlex.printStackTrace();   
        }  
		
		ArrayList<String> userIds = new ArrayList<String>();
		ArrayList<String> loanInfoIds = new ArrayList<String>();
		
		try {
			String sql_userId = "select distinct user_id from (SELECT b.id, c.grant_date, date_format(c.grant_date,'%Y%m%d'), hour(c.grant_date), b.user_id, b.account_id, b.loan_info_id, b.copies, b.invest_amount, b.loan_info_title, c.loan_period, 'dw_biz_invest_statements' source_table, ifnull(bid_method,'web'), case when substr(d.salesman,1,2)='CF' and c.loan_period!=16 and c.enlending_type!=4 and substr(b.pay_day,1,6) BETWEEN 201503 and 201510  then 1 when substr(d.salesman,1,2)='CF' and substr(b.pay_day,1,6) BETWEEN 201511 and 201601 and c.loan_period>20 then 1 when substr(d.salesman,1,2)='CF' and b.pay_day BETWEEN 20160201 and 20160614 and c.loan_period>=15 then 1 when substr(d.salesman,1,2)='CF' and b.pay_day>20160614 and c.loan_period>=30 then 1 else 0 end, 'thw' FROM DW.dw_biz_invest_statements b,DW.dw_biz_loan_info c,DW.ods_sys_user d where b.loan_info_id = c.id and b.user_id=d.id) k";
			String sql_loanInfoIdString = "select distinct loan_info_id from (SELECT b.id, c.grant_date, date_format(c.grant_date,'%Y%m%d'), hour(c.grant_date), b.user_id, b.account_id, b.loan_info_id, b.copies, b.invest_amount, b.loan_info_title, c.loan_period, 'dw_biz_invest_statements' source_table, ifnull(bid_method,'web'), case when substr(d.salesman,1,2)='CF' and c.loan_period!=16 and c.enlending_type!=4 and substr(b.pay_day,1,6) BETWEEN 201503 and 201510  then 1 when substr(d.salesman,1,2)='CF' and substr(b.pay_day,1,6) BETWEEN 201511 and 201601 and c.loan_period>20 then 1 when substr(d.salesman,1,2)='CF' and b.pay_day BETWEEN 20160201 and 20160614 and c.loan_period>=15 then 1 when substr(d.salesman,1,2)='CF' and b.pay_day>20160614 and c.loan_period>=30 then 1 else 0 end, 'thw' FROM DW.dw_biz_invest_statements b,DW.dw_biz_loan_info c,DW.ods_sys_user d where b.loan_info_id = c.id and b.user_id=d.id) k";
			
			statement = conn.createStatement();
			ResultSet rsUserId = statement.executeQuery(sql_userId);
			
			while (rsUserId.next()) {
				String userId = rsUserId.getString(1);
				userIds.add(userId);	
			}
			
			ResultSet rsLoanInfoId = statement.executeQuery(sql_loanInfoIdString);
			while (rsLoanInfoId.next()) {
				String loanInfoId = rsLoanInfoId.getString(1);
				loanInfoIds.add(loanInfoId);	
			}
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int[] rand5000 = randomGet000(userIds.size(), 5000);
		int[] rand4000 = randomGet000(loanInfoIds.size(), 4000);
		
		SimpleDateFormat time = new SimpleDateFormat("yyyyMMddHHmmss");
		String startTime = "20160801000000";
		Long currentTime = time.parse(startTime).getTime();
		
		Thread[] threads = new Thread[40];
		
		for(long t = 0; t < 40; t++) {
			try {
				Connection connection = DriverManager.getConnection( urlInsert, username, password);
				threads[(int) t] = new Thread(new WorkRunnable(connection, currentTime + t * 500000 * 1000, (int) t, rand5000, rand4000, userIds, loanInfoIds) );
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
		
	}
	
	static class WorkRunnable implements Runnable {
		private Connection conn;
		private long startTime;
		private int index;
		private int[] rand5000;
		private int[] rand4000;
		private ArrayList<String> userIds;
		private ArrayList<String> loanInfoIds;
		private PreparedStatement st;
		
		WorkRunnable(Connection conn, long startTime, int index, int[] rand5000, int[] rand4000, ArrayList<String> userIds, ArrayList<String> loanInfoIds) throws SQLException {
			this.conn = conn;
			this.startTime = startTime;
			this.index = index;
			this.rand5000 = rand5000;
			this.rand4000 = rand4000;
			this.userIds = userIds;
			this.loanInfoIds = loanInfoIds;
			st = conn.prepareStatement("insert into dw_biz_invest_statements_qianwan (id, create_date, create_day, create_hour, user_id, account_id, loan_info_id, loan_info_type, loan_info_title, loan_info_interest, pay_time, pay_day, pay_hour, copies, invest_amount, yeepay_request_no, principal, interest, new_loan_info_id, new_loan_info_type, is_zq_callbacked, bid_method, storm_time) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		}
		
		@Override
		public void run() {
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMddHHmmss");
				for(int j = index * 100; j < (index + 1) * 100; j++) {
					try{
						for (int i : rand5000) {
							String strTime = time.format(startTime);
							st.setString(1, strTime + string2MD5(strTime));
							st.setTimestamp(2, new Timestamp(startTime));
							st.setString(3, strTime.substring(0, 8));
							st.setString(4, String.valueOf(new Date(startTime).getHours()));
							st.setString(5, userIds.get(i));
							st.setString(6, "taihe2014090001327th");
							st.setString(7, loanInfoIds.get(rand4000[j]));
							st.setString(8, "3");
							st.setString(9, "消费");
							st.setString(10, "15.00");
							st.setTimestamp(11, new Timestamp(startTime));
							st.setString(12, strTime.substring(0, 8));
							st.setString(13, String.valueOf(new Date(startTime).getHours()));
							st.setBigDecimal(14, new BigDecimal(50));
							st.setBigDecimal(15, new BigDecimal(100));
							st.setString(16, "lsh2014090000047");
							st.setBigDecimal(17, new BigDecimal(1));
							st.setBigDecimal(18, new BigDecimal(2));
							st.setString(19, "20151222153556479ee351987bc6b4fc79984a271a3e9b5e5");
							st.setString(20, "9");
							st.setString(21, "N");
							st.setString(22, "phone_A");
							st.setTimestamp(23, new Timestamp(startTime));
							st.addBatch();
							startTime += 1000;
						}
						st.executeBatch();
					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}
				}	
		}
		
	}

}
