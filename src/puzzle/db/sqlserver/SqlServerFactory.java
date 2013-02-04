package puzzle.db.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlServerFactory 
{

	public static void main(String[] args)
	{
		try
		{
			String ip = "67.90.142.100";
			String db = "admin";
			String user = "bdonges";
			String pass = "w@reagl3";
			
			SqlServerFactory f = new SqlServerFactory();
			Connection c = f.createConnection(ip, db, user, pass);
			System.out.println("is connection closed? " + c.isClosed());
		}
		catch (Exception e)
		{
			System.err.println("exception - SqlServerFactory main() - message: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param url
	 * @param user
	 * @param pass
	 * @return
	 * @throws SQLException
	 */
	public Connection createConnection(String ip, String db, String user, String pass) throws SQLException 
	{
		return DriverManager.getConnection("jdbc:sqlserver://"+ip+";databaseName="+db+";", user, pass);
	}
	
}
