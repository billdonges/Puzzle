package puzzle.populate;

import java.sql.Connection;

import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

import puzzle.db.mongo.MongoFactory;
import puzzle.db.mysql.MySqlFactory;
import puzzle.db.sqlserver.SqlServerFactory;

public class CreateAndPopulate 
{

	public static int NONE = 0;
	public static int CREATE_RDBMS = 1;
	public static int CREATE_MONGO = 2;
	public static int CREATE_ALL = 4;
	public static int CREATE_RDBMS_TABLES = 9;
	public static int CLEAN_RDBMS = 5;
	public static int CLEAN_MONGO = 6;
	public static int CLEAN_ALL = 8;
	
	public static int MYSQL = 100;
	public static int MSSQL = 101;
	
	public static void main(String[] args)
	{
		try
		{
			int action = NONE;
			int realmMultiplier = 10;
			int dbType = 0;
			
			if (args.length == 0)
			{
				throw new Exception("you must enter an action arguement - CREATE_RDBMS, CREATE_MONGO, CREATE_ALL, CLEAN_RDBMS, CLEAN_MONGO or CLEAN_ALL");
			}
			else
			{
				if (args[0].equalsIgnoreCase("CREATE_RDBMS"))
					action = CREATE_RDBMS;
				else if (args[0].equalsIgnoreCase("CREATE_MONGO"))
					action = CREATE_MONGO;
				else if (args[0].equalsIgnoreCase("CREATE_ALL"))
					action = CREATE_ALL;
				else if (args[0].equalsIgnoreCase("CLEAN_RDBMS"))
					action = CLEAN_ALL;
				else if (args[0].equalsIgnoreCase("CLEAN_MONGO"))
					action = CLEAN_ALL;				
				else if (args[0].equalsIgnoreCase("CLEAN_ALL"))
					action = CLEAN_ALL;
				else
					action = NONE;
			}
			
			if (args[1] == null)
			{
				throw new Exception("you must pass a rdbms type - MYSQL or MSSQL");
			}
			else 
			{
				if (args[1].equalsIgnoreCase("MYSQL"))
					dbType = MYSQL;
				else if (args[1].equalsIgnoreCase("MSSQL"))
					dbType = MSSQL;
			}
			
			if (action == CREATE_RDBMS || action == CREATE_ALL)
			{
				if (args[2] != null)
				{
					try
					{
						realmMultiplier = Integer.parseInt(args[2]);
					}
					catch (Exception e)
					{
						System.err.println("error converting the realm muliplier - is "+args[2]+" able to be converted to an int?");
						e.printStackTrace();
					}
				}
			}
			
			if (action != NONE)
				new CreateAndPopulate().run(action, dbType, realmMultiplier);
			else
				System.out.println("nothing to run - action is " + NONE);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void run(int action, int dbType, int realmMultiplier) throws Exception
	{
		// initialize 
		System.out.println("starting createandpopulate running action "+action);
		
		// get mongo connection 
		MongoFactory mf = new MongoFactory();
		Mongo mongo = mf.getConnection("192.168.23.28", 27017);
		mongo.setWriteConcern(WriteConcern.SAFE);
		System.out.println("got mongo connection");
		
		// get mysql connection
		MySqlFactory mySqlF = new MySqlFactory();
		//Connection mysql = mySqlF.getConnection("192.168.23.28", "<db>", "<user>", "<pass>");
		Connection mysql = null;
		//System.out.println("got mysql connection, closed? "+mysql.isClosed());
		
		// get mssql connection
		SqlServerFactory msSqlF = new SqlServerFactory();
		Connection mssql = msSqlF.createConnection("192.168.23.114", "whatcounts", "sa", "dev=horse.play");
		//Connection mssql = null;
		
		// get Database object
		Database db = new Database(realmMultiplier);
		
		System.out.println("start working at " + new java.util.Date());
		
		// create data
		if (action == CREATE_RDBMS)
		{
			createRDBMS(dbType, mysql, mssql, db);
		}
		else if (action == CREATE_MONGO)
		{
			createMongo(dbType, mongo, mysql, mssql, db);
		}
		else if (action == CREATE_ALL)
		{
			createAll(dbType, mysql, mssql, mongo, db);
		}
		else if (action == CLEAN_RDBMS)
		{
			cleanRDBMS(dbType, mysql, mssql);
		}
		else if (action == CLEAN_MONGO)
		{
			cleanMongo(mongo, db);
		}		
		else if (action == CLEAN_ALL)
		{
			cleanAll(dbType, mysql, mssql, mongo, db);
		}
		
		System.out.println("done working at " + new java.util.Date());
		
		// close connections
		try
		{
			mongo.close();			
			mysql.close();
			mssql.close();
		}
		catch (Exception e)
		{
		
		}
	}
	
	private void createRDBMS(int dbType, Connection mysql, Connection mssql, Database db) throws Exception
	{
		if (dbType == MYSQL)
		{
			CreateAndPopulateMySQL cpMySQL = new CreateAndPopulateMySQL();
			cpMySQL.insertSubscribers(mysql, db);
			cpMySQL.insertTracking(mysql, db);	
		}
		else if (dbType == MSSQL)
		{
			CreateAndPopulateMSSQL cpMSSQL = new CreateAndPopulateMSSQL();
			cpMSSQL.checkTables(mssql);
			cpMSSQL.insertSubscribers(mssql, db);
			cpMSSQL.insertTracking(mssql, db);
		}
	}
	
	private void createMongo(int dbType, Mongo mongo, Connection mysql, Connection mssql, Database db) throws Exception
	{
		CreateAndPopulateMongo cpMongo = new CreateAndPopulateMongo();
		
		Connection con = null;
		if (dbType == MYSQL)
			con = mysql;
		else if (dbType == MSSQL)
			con = mssql;
		
		cpMongo.populateSADCollections(mongo, con, db);
	}
	
	/**
	 * populates data in both rdbms (subscribers and tracking) and mongo (sad and relational)
	 * @param dbType
	 * @param mysql
	 * @param mssql
	 * @param mongo
	 * @param db
	 * @throws Exception
	 */
	private void createAll(int dbType, Connection mysql, Connection mssql, Mongo mongo, Database db) throws Exception
	{
		createRDBMS(dbType, mysql, mssql, db);
		createMongo(dbType, mongo, mysql, mssql, db);
	}
	
	/**
	 * removes data from rdbms (subscribers and tracking)
	 * @param dbType
	 * @param mysql
	 * @param mssql
	 * @throws Exception
	 */
	private void cleanRDBMS(int dbType, Connection mysql, Connection mssql) throws Exception
	{
		if (dbType == MYSQL)
		{
			CreateAndPopulateMySQL cpMySQL = new CreateAndPopulateMySQL();
			cpMySQL.clean(mysql);
		}
		else if (dbType == MSSQL)
		{
			CreateAndPopulateMSSQL cpMSSQL = new CreateAndPopulateMSSQL();
			cpMSSQL.clean(mssql);
		}
	}
	
	/**
	 * removes data from mongo db (sad and relational)
	 */
	private void cleanMongo(Mongo mongo, Database db)
	{
		CreateAndPopulateMongo cpMongo = new CreateAndPopulateMongo();
		cpMongo.removeExistingDocuments(mongo, db);
	}
	
	/**
	 * removes data from rdbms (subscriber and tracking) and mongo (sad and relational) 
	 * @param dbType
	 * @param mysql
	 * @param mssql
	 * @throws Exception
	 */
	private void cleanAll(int dbType, Connection mysql, Connection mssql, Mongo mongo, Database db) throws Exception
	{
		cleanRDBMS(dbType, mysql, mssql);
		cleanMongo(mongo, db);
	}
}

