package puzzle.populate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Vector;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

import puzzle.db.mongo.MongoFactory;
import puzzle.populate.Database.Realm;

public class CreateAndPopulateMongo 
{

	public static int NOTHING 			= 0;
	public static int INSERT_SAD		= 1;
	public static int INSERT_RELATIONAL = 6;
	public static int INSERT_CUSTOM		= 7;
	public static int UPDATE 			= 2;
	public static int DELETE			= 3;
	public static int SHOW   			= 4;
	public static int GET 				= 5;
	
	public static void main(String[] args)
	{
		try
		{
			CreateAndPopulateMongo cp = new CreateAndPopulateMongo();
			
			String dbName 			= "rlm1";
			String collectionName 	= "relational";
			int numOfDocs 			= 100;
			int action 				= CreateAndPopulateMongo.INSERT_RELATIONAL;
			String ipAddress		= "192.168.0.106";
			int port				= 27017;

			if (dbName.equals("")) 
				System.err.println("dbname must not be \"\"");
			else if (collectionName.equals(""))
				System.err.println("collectionName must not be \"\"");
			else
				cp.run(ipAddress, port, dbName, collectionName, numOfDocs, action);
			
		}
		catch (Exception e)
		{
			System.err.println("exception - message: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 
	 * @param dbName
	 * @param collectionName
	 * @param numOfDocs
	 * @param action
	 * @param removeExisting
	 * @throws Exception
	 */
	public void run(String ipAddress, int port, String dbName, String collectionName, int numOfDocs, int action) throws Exception
	{
		System.out.println("start time: " + new java.util.Date());
		
		// create mongo factory instance
		MongoFactory mf = new MongoFactory();
		
		// get connection
		Mongo m = mf.getConnection(ipAddress, port);
		m.setWriteConcern(WriteConcern.SAFE);
		
		// create and/or get db
		DB db = createOrGetDb(mf, m, dbName);
		
		// create and/or get col
		DBCollection col = createOrGetCollection(db, collectionName);

		//------------------------------------------------------------------------------
		System.out.println("number of documents in "+col.getName()+" in db " + db.getName() +" before action:  "+col.count());		
		if (action == INSERT_SAD)
		{
			col = removeExistingDocuments(col);
			col = populateSADCollection(col, numOfDocs);
		}
		else if (action == INSERT_RELATIONAL)
		{
			DBCollection sCol = createOrGetCollection(db, "sad"); 
			
			col = removeExistingDocuments(col);
			col = populateRelationalCollection(col, sCol);
		}
		else if (action == INSERT_RELATIONAL)
		{
			DBCollection sCol = createOrGetCollection(db, "sad"); 
			
			col = removeExistingDocuments(col);
			col = populateRelationalCollection(col, sCol);
		}		
		else if (action == UPDATE)
		{
			col = upsertCollection(col, numOfDocs);
		}
		else if (action == NOTHING)
		{
			System.out.println("doing nothing and doing it well");
		}
		else if (action == SHOW)
		{
			showDocsInCollection(col);
		}
		else if (action == DELETE)
		{
			col = removeExistingDocuments(col);
		}
		else if (action == GET)
		{
			getData(mf, col, "teams");
		}
		
		System.out.println("number of documents in "+col.getName()+" after action:    "+col.count());
		System.out.println("end time: " + new java.util.Date());

		mf.close(db);
	}
	
	
	/**
	 * 
	 * @param mf
	 * @param col
	 * @param index
	 * @throws Exception
	 */
	public void getData(MongoFactory mf, DBCollection col, String index) throws Exception
	{
		
		mf.createIndex(col, "purchase_orders.items.amount", 1);
		
		BasicDBObject query = new BasicDBObject();
		query.put("purchase_orders.items.amount","$1300.00");
		DBCursor cur = col.find(query);
		System.out.println("how many records found in get? "+cur.size());
		try 
		{
			while(cur.hasNext()) 
			{
				System.out.println(cur.next());
			}
		} 
		finally 
		{
			cur.close();
		}
		
		mf.dropIndex(col, "purchase_orders.items.amount"+"_1");
	}

	/**
	 * 
	 * @param mf
	 * @param m
	 * @param dbName
	 * @return
	 */
	public DB createOrGetDb(MongoFactory mf, Mongo m, String dbName)
	{
		return mf.getDatabase(m, dbName);
	}
	
	/**
	 * 
	 * @param db
	 * @param collectionName
	 * @return
	 */
	public DBCollection createOrGetCollection(DB db, String collectionName) 
	{
		return db.getCollection(collectionName);
	}
	
	/**
	 * 
	 * @param col
	 * @param numOfDocs
	 * @return
	 */
	public DBCollection populateSADCollection(DBCollection col, int numOfDocs) 
	{
		System.out.println("starting populate of collection "+col.getName()+" at "+new java.util.Date());
		Vector<String> teams = new Vector<String>();
		teams.add("auburn tigers");
		teams.add("atlanta falcons");
		teams.add("atlanta braves");
		teams.add("purdue boilermakers");
		teams.add("georgia bulldogs");
		int teamCnt = 0;
		
		for (int i = 1; i <= numOfDocs; i++)
		{
			BasicDBObject sad = new BasicDBObject();
			sad.put("_id",i);
			sad.put("email","bill"+i+"@mail.com");
			sad.put("first_name", "bill");
			sad.put("last_name", "donges");
			sad.put("age", 38);
			
			BasicDBObject custom = new BasicDBObject();
			int kids = 1;
			int cars = 1;
			if (i % 2 == 0) 
			{ 
				kids = 2;
				cars = 2;
			}
			custom.put("kids",kids);
			custom.put("cars",cars);
			custom.put("houses",1);
			sad.put("custom",custom);
			
			ArrayList<String> team = new ArrayList<String>();
			team.add(teams.get(teamCnt));
			teamCnt++;
			if (teamCnt == 4) { teamCnt = 0; }
			sad.put("teams", team);
			
			col.insert(sad);
		}
		System.out.println("ending populate of collection "+col.getName()+" at "+new java.util.Date());
		return col;
	}
	
	/**
	 * 
	 * @param db
	 * @param con
	 * @throws Exception
	 */
	public void populateSADCollections(Mongo mongo, Connection con, Database db) throws Exception 
	{
		// set up rdbms query
		PreparedStatement pst = con.prepareStatement("SELECT id FROM subscribers WHERE realm_id = ?");

		// for each realm...
		for (int i = 0; i < db.realms.size(); i++)
		{
			// get realm...
			Realm r = db.realms.get(i);

			// and set up mongo connection to a db named the same as the realm...
			DB mongoDb = mongo.getDB(r.getName());
			DBCollection col = createOrGetCollection(mongoDb, "sad");
			
			// then get all subscribers for that realm in rdbms and create a sad document for them
			pst.setInt(1, r.getId());
			ResultSet rs = pst.executeQuery();
			while (rs.next())
				insertSAD(col, rs.getInt("id"));
		}
	}	

	/**
	 * 
	 * @param col
	 * @param subscriberId
	 * @return
	 * @throws Exception
	 */
	public DBCollection insertSAD(DBCollection col, int subscriberId) throws Exception 
	{
		BasicDBObject sad = new BasicDBObject();
		sad.put("_id",subscriberId);
		sad.put("age", 38);
				
		BasicDBObject custom = new BasicDBObject();
		int kids = 1;
		int cars = 1;
		if (subscriberId % 2 == 0) 
		{ 
			kids = 2;
			cars = 2;
		}
		custom.put("kids",kids);
		custom.put("cars",cars);
		custom.put("houses",1);
		sad.put("custom",custom);
			
		col.insert(sad);

		return col;
	}		
	
	/**
	 * 
	 * @param col
	 * @return
	 */
	public DBCollection populateRelationalCollection(DBCollection rCol, DBCollection sCol)
	{
		System.out.println("starting populate of collection "+rCol.getName()+" at "+new java.util.Date());
		DBCursor cur = sCol.find();

		System.out.println("cursor size is " + cur.size());
		
		int j = 1;
		while (cur.hasNext())
		{
			 BasicDBObject sad = (BasicDBObject) cur.next();
			
			// set up relational db
			BasicDBObject relational = new BasicDBObject();
			relational.put("_id", sad.get("_id"));
			
			// set up purchase order object
			ArrayList<BasicDBObject> orders = new ArrayList<BasicDBObject>();
			BasicDBObject order = new BasicDBObject();
			order.put("order_id", j);
			order.put("order_date", "1/1/2012");
			order.put("amount","$"+j+".00");

			ArrayList<BasicDBObject> orderItems = new ArrayList<BasicDBObject>();
			for (int k = 0; k < 3; k++)
			{
				String itemName = "phone";
				String itemAmount = "$100.00";
				
				if (k == 1) {
					itemName = "tablet";
					itemAmount = "$300.00";
				} else if (k == 2) {
					itemName = "tv";
					itemAmount = "$1300.00";
				}
				
				BasicDBObject orderItem = new BasicDBObject();
				orderItem.put("item_id", j+"_"+k);
				orderItem.put("item_name", itemName);
				orderItem.put("amount", itemAmount);
				orderItems.add(orderItem);
			}
			
			// add order items to items array in order
			order.put("items",orderItems);
			orders.add(order);
			
			relational.put("purchase_orders", orders);
			j++;
			
			rCol.insert(relational);
		}
		System.out.println("ending populate of collection "+rCol.getName()+" at "+new java.util.Date());
		return rCol;
	}
	
	/**
	 * 
	 * @param col
	 * @param numOfDocs
	 * @return
	 */
	public DBCollection upsertCollection(DBCollection col, int numOfDocs) 
	{
		System.out.println("starting populate of collection "+col.getName()+" at "+new java.util.Date());
		for (int i = 1; i <= numOfDocs; i++)
		{
			BasicDBObject sad = new BasicDBObject();
			sad.append("_id",i);
			BasicDBObject custom = new BasicDBObject().append("$set", new BasicDBObject().append("custom", new BasicDBObject("pets",1)));
			
			col.update(sad, custom, true, true);

			if (i % 1000 == 0)
				System.out.println("on row "+i);
		}
		System.out.println("ending populate of collection "+col.getName()+" at "+new java.util.Date());
		return col;
	}	
	
	/**
	 * 
	 * @param col
	 * @return
	 */
	public void removeExistingDocuments(Mongo mongo, Database db)
	{
		// for each realm...
		for (int i = 0; i < db.realms.size(); i++)
		{
			// get realm...
			Realm r = db.realms.get(i);

			// and set up mongo connection to a db named the same as the realm...
			DB mongoDb = mongo.getDB(r.getName());
			DBCollection col = createOrGetCollection(mongoDb, "sad");
			removeExistingDocuments(col);
		}
	}
	
	/**
	 * 
	 * @param col
	 * @return
	 */
	public DBCollection removeExistingDocuments(DBCollection col)
	{
		col.remove(new BasicDBObject());
		return col;
	}
	
	/**
	 * 
	 * @param col
	 */
	public void showDocsInCollection(DBCollection col) 
	{
		for (int i = 0; i < col.count(); i++)
		{
			DBCursor cur = col.find();
			while (cur.hasNext())
				System.out.println(cur.next().toString());
		}
	}
}
