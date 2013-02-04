package puzzle.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import puzzle.db.mongo.MongoFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class DbManagementAction 
{

	private DBMgmt dbm_;
	private MongoFactory mcf_;

	public DbManagementAction(DBMgmt dbm)
	{
		mcf_ = new MongoFactory();
		dbm_ = dbm;
	}	
	
	/**
	 * 
	 * @param db
	 */
	public void close(DB db) throws Exception
	{
		mcf_.close(db);
	}
	
	/**
	 * 
	 * @return
	 */
	public Mongo getMongo() throws Exception
	{
		return mcf_.getConnection(dbm_.getDbLocation(), dbm_.getDbPort());
	}
	
	/**
	 * 
	 * @param dbName
	 * @return
	 * @throws Exception
	 */
	public DB getDB(String dbName) throws Exception
	{
		DB db = null;
		if (dbm_.isUseToAuth())
		{
			if (dbm_.getAuthDB() == null)
			{
				dbm_.setAuthDB(mcf_.getDatabase(dbm_.getDbLocation(), dbm_.getAuthDbName(), dbm_.getDbPort(), dbm_.isValidate(), dbm_.getDbUsername(), dbm_.getDbPassword()));
				db = dbm_.getAuthDB().getMongo().getDB(dbName);
			}
			else
			{
				db = dbm_.getAuthDB().getMongo().getDB(dbName);
			}
		}
		else
		{
			db = mcf_.getDatabase(dbm_.getDbLocation(), dbName, dbm_.getDbPort(), dbm_.isValidate(), dbm_.getDbUsername(), dbm_.getDbPassword());
		}
		return db;
	}
	
	/**
	 * 
	 * @param dbs
	 * @return
	 */
	public List<Stats> getDbStats() throws Exception
	{
		List<String> dbs = getDbs();
		List<Stats> stats = new ArrayList<Stats>();
		for (int i = 0; i < dbs.size(); i++)
		{
			stats.add(getDbStats(dbs.get(i)));
		}
		return stats;
	}
	
	/**
	 * 
	 * @param db
	 * @return
	 */
	public Stats getDbStats(String dbName) throws Exception
	{
		DB db = getDB(dbName);
		return new Stats(db.getStats());
	}

	/**
	 * 
	 * @param col
	 * @return
	 */
	public List<String> getIndexNames(DBCollection col) 
	{
        List<DBObject> list = col.getIndexInfo();
        List<String> names = new ArrayList<String>();
        for (DBObject o : list) {
            names.add(o.toString());
        }
        return names;
	}
	
	/**
	 * 
	 * @return
	 */
	public List<String> getDbs() throws Exception
	{
		DB db = getDB(dbm_.getDbName());
		List<String> dbs = db.getMongo().getDatabaseNames();
		return dbs;
	}
	
	/**
	 * 
	 * @param dbName
	 * @return
	 */
	public List<String> getCols(String dbName) throws Exception
	{
		DB db = getDB(dbName);
		Set<String> set = db.getCollectionNames();
		List<String> cols = new ArrayList<String>();
		for (String s : set) 
		{
			cols.add(s);
		}
		return cols;
	}
	
	/**
	 * 
	 * @param dbName
	 * @param colName
	 * @return
	 */
	public List<String> getRows(String dbName, String colName, int pagenum, int nPerPage) throws Exception
	{
		List<String> rows = new ArrayList<String>();
		DB db = getDB(dbName);
		DBCollection col = db.getCollection(colName);
		DBCursor cur = col.find().skip((pagenum-1)*nPerPage).limit(nPerPage);
		while (cur.hasNext())
		{
			String s = cur.next().toString();
			rows.add(s);
		}
		return rows;
	}
	
	/**
	 * 
	 * @param dbName
	 * @param colName
	 * @return
	 * @throws Exception
	 */
	public int getRowCount(String dbName, String colName) throws Exception
	{
		return (int) getDB(dbName).getCollection(colName).count();
	}
	
	public List<String> getSearchRows(String dbName, String colName, String searchString) throws Exception
	{
		List<String> rows = new ArrayList<String>();
		if ((searchString != null) && (searchString.length() > 0))
		{
			DB db = getDB(dbName);
			DBCollection col = db.getCollection(colName);
			DBObject obj = (DBObject) JSON.parse(searchString);
			DBCursor cur = col.find(obj);
			while (cur.hasNext())
			{
				String s = cur.next().toString();
				rows.add(s);
			}
		}
		return rows;
	}

}
