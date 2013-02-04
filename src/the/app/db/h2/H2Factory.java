package the.app.db.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;

public class H2Factory 
{

	public void close(Connection c) throws Exception
	{
		c.close();
	}
	
	/**
	 * 
	 * @param dbName
	 * @param username
	 * @param passwd
	 * @return
	 * @throws Exception
	 */
	public Connection getConnection(String dbName, String username, String passwd) throws Exception
	{
        Class.forName("org.h2.Driver");
        return DriverManager.getConnection("jdbc:h2:~/"+dbName, username, passwd);
	}
	
	/**
	 * 
	 * @param tableName
	 * @param dbName
	 * @param username
	 * @param passwd
	 * @throws Exception
	 */
	public void createTable(String tableName, String dbName, String username, String passwd) throws Exception
	{
		createTable(getConnection(dbName, username, passwd), tableName, new Hashtable<String,String>());
	}
	
	/**
	 * 
	 * @param c
	 * @param tableName
	 * @throws Exception
	 */
	public void createTable(Connection c, String tableName, Hashtable <String,String> columns) throws Exception
	{
		
		StringBuffer sql = new StringBuffer();
		sql.append("CREATE TABLE IF NOT EXISTS "+tableName+" ");
		sql.append("(");
		
		String cols = "";
		Enumeration <String> names = columns.keys();
		while (names.hasMoreElements())
		{
			String name = names.nextElement().toString();
			cols = cols + name + " " + columns.get(name).toString() + ",";
		}
		cols = cols.substring(0,cols.length()-1);
		sql.append(cols);
		sql.append(")");
		
		System.out.println("create table: definition:");
		System.out.println(sql.toString());
		
		Statement s = c.createStatement();
		s.execute(sql.toString());
		s.close();
	}
	
	public static void main(String [] args)
	{
		try
		{
			System.out.println("getting factory");
			H2Factory f = new H2Factory();
			System.out.println(" ");
			
			System.out.println("getting connection");
			Connection c = f.getConnection("rlm1", "sa", "dev=horse.play");
			System.out.println("got connection.  is it closed? " + c.isClosed());
			System.out.println(" ");
			
			System.out.println("create temp table");
			Hashtable <String,String> h = new Hashtable<String,String>();
			h.put("id", "int");
			h.put("name", "varchar(50)");
			h.put("code","varchar(10)");
			f.createTable(c, "temp_users", h);
			System.out.println(" ");
			
			System.out.println("insert into temp_users");
			Statement s = c.createStatement();
			s.execute("INSERT INTO temp_users (id, name, code) values (1,'bill','A30SD343')");
			System.out.println("inserted into temp_users.");
			System.out.println(" ");
			
			System.out.println("get rows from temp_users");
			ResultSet rs = s.executeQuery("SELECT * FROM temp_users");
			while (rs.next())
			{
				System.out.println("  " + rs.getInt("id")+" :: " + rs.getString("name") + " :: " + rs.getString("code"));
			}
			System.out.println(" ");
			
			System.out.println("closing connection");
			f.close(c);
			System.out.println("closed connection.  is it really closed? " + c.isClosed());
			System.out.println(" ");
		}
		catch (Exception e)
		{
			System.err.println("exception - message: " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
}
