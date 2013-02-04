package the.app.db.mongo;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class MongoFactory 
{
	
	/**
	 * closes the mongo object inside the db object, then nulls out the db object
	 * @param db
	 */
	public void close(DB db)
	{
		if (db != null)
		{
			db.getMongo().close();
			db = null;
		}
	}
	
	/**
	 * 
	 * @param dbLocation
	 * @param dbName
	 * @param dbPort
	 * @return
	 * @throws Exception
	 */
	public DB getDatabase(String dbLocation, String dbName, int dbPort) throws Exception
	{
		return getDatabase(dbLocation, dbName, dbPort, false, "", "");
	}
	
	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public Morphia getMorphia() throws Exception
	{
		Morphia morphia = new Morphia();
		return morphia;
	}
	
	/**
	 * 
	 * @param mongo
	 * @param morphia
	 * @param dbName
	 * @return
	 * @throws Exception
	 */
	public Datastore getDatastore(Mongo mongo, Morphia morphia, String dbName) throws Exception
	{
		Datastore ds = morphia.createDatastore(mongo, dbName);
		return ds;
	}

	/**
	 * 
	 * @param dbLocation
	 * @param dbPort
	 * @param dbName
	 * @return
	 * @throws Exception
	 */
	public Datastore getDatastore(String dbLocation, int dbPort, String dbName) throws Exception
	{
		Mongo mongo = getConnection(dbLocation, dbPort);
		Datastore ds = getMorphia().createDatastore(mongo, dbName);
		return ds;
	}
	
	/**
	 * dbLocation should be defined in the.app.AppInfo
	 * dbName should be defined in the.app.AppInfo
	 * dbPort should be defined in the.app.AppInfo
	 * @param dbLocation
	 * @param dbName
	 * @param dbPort
	 * @return DB
	 * @throws Exception 
	 */
	public DB getDatabase(String dbLocation, String dbName, int dbPort, boolean validate, String username, String password) throws Exception
	{
		Mongo m = getConnection(dbLocation, dbPort);
		DB db = getDatabase(m, dbName);
		if (validate)
			if (!auth(db, username, password))
				throw new Exception("username / password / db can not be authorized");
		return db;
	}
	
	/**
	 * 
	 * @param m
	 * @param dbName
	 * @return
	 */
	public DB getDatabase(Mongo m, String dbName) 
	{
		return m.getDB(dbName);
	}	
	
	/**
	 * 
	 * @param db
	 * @param username
	 * @param passwd
	 * @return
	 */
	public boolean auth(DB db, String username, String passwd)
	{
		return db.authenticate(username, passwd.toCharArray());
	}

	/**
	 * creates an index on a collection  
	 * 
	 * col   = DBCollection to create the index on
	 * field = field to add the index on
	 * order = 1 for ascending, -1 for decending
	 * 
	 * @param col 
	 * @param field
	 * @param orderType 
	 * @throws Exception
	 */
	public DBCollection createIndex(DBCollection col, String field, int order) throws Exception
	{
		if (order == 1 || order == -1) {
			col.createIndex(new BasicDBObject(field, order));
		} else {
			throw new Exception ("The order must be set to either 1 (asc) or -1 (desc)");
		}
		return col;
	}

	/**
	 * 
	 * @param col
	 */
	public void showIndexes(DBCollection col)
	{
		List<DBObject> indexes = col.getIndexInfo();
		System.out.println("how many indexes are there for "+col.getName()+" : "+indexes.size());
		for (DBObject o : indexes)
		{
			System.out.println("index dbobject:  "+o);
		}
	}
	
	/**
	 * 
	 * @param col
	 * @param indexName
	 * @return
	 */
	public DBCollection dropIndex(DBCollection col, String indexName) 
	{
		col.dropIndex(indexName);
		return col;
	}
	
	/**
	 * 
	 * @param dbLocation
	 * @param dbPort
	 * @return m
	 * @throws Exception
	 */
	public Mongo getConnection(String dbLocation, int dbPort) throws Exception
	{
		Mongo m = new Mongo(dbLocation , dbPort);
		m.setWriteConcern(WriteConcern.SAFE);
		if (!mongoRunningAt(m))
			throw new Exception ("cannot connect to mongo database at "+dbLocation+" on port "+dbPort);	
		return m;
	}
	
	/**
	 * 
	 * @param m
	 * @return boolean
	 */
	public boolean mongoRunningAt(Mongo m) 
	{
		boolean connected = true;
	    try 
	    {
		    Socket socket = m.getMongoOptions().socketFactory.createSocket();
		    socket.connect(m.getAddress().getSocketAddress());
		    socket.close();
		} 
	    catch (IOException ex) 
	    {
		    connected = false;
		}
		return connected;
	}	
	
}
