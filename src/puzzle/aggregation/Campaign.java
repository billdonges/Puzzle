package puzzle.aggregation;

import java.util.List;

import puzzle.db.mongo.MongoFactory;
import puzzle.populate.CreateAndPopulateMongo;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class Campaign 
{

	public static int NOTHING 		= 0;
	public static int DELETE 		= 5;
	public static int ADD_CAMPAIGN  = 6;
	public static int GET_CAMPAIGN	= 7;
	public static int GET_CAMPAIGNS	= 8;
	public static int NEXT_ID		= 9;
	public static int REBUILD	    = 10;
	
	public static void main(String[] args)
	{
		try
		{
			Campaign c = new Campaign();
			
			String dbName 			= "big";
			String collectionName 	= "campaign_sends";
			int numOfDocs			= 0;
			int numOfCampaigns		= 730;
			int low 				= 10000;
			int high				= 100000;
			int action 				= Campaign.GET_CAMPAIGN;
			int campaignId			= 538;

			c.run(dbName, collectionName, numOfDocs, numOfCampaigns, action, low, high, campaignId);
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
	public void run(String dbName, 
				    String collectionName, 
				    int numOfDocs, 
				    int numOfCampaigns, 
				    int action, 
				    int low, 
				    int high, 
				    int campaignId) throws Exception
	{
		System.out.println("start time: " + new java.util.Date());
		
		// create mongo factory instance
		MongoFactory mf = new MongoFactory();
		
		// get connection
		Mongo m = mf.getConnection("localhost", 27017);
		m.setWriteConcern(WriteConcern.SAFE);

		// create the createandpopulate instance
		CreateAndPopulateMongo cap = new CreateAndPopulateMongo(); 
		
		// create and/or get db
		DB db = cap.createOrGetDb(mf, m, dbName);
		
		// create and/or get col
		DBCollection col = cap.createOrGetCollection(db, collectionName);

		//------------------------------------------------------------------------------		
		System.out.println("number of documents in "+col.getName()+" in db " + db.getName() +" before action " + action + ":  "+col.count());		
		if (action == ADD_CAMPAIGN)
		{
			for (int i = 0; i < numOfCampaigns; i++)
			{
				addCampaignToCollection(mf, col, numOfDocs, low, high);
				if (i % 10 == 0) { System.out.println("      on "+i); }
			}
		}
		else if (action == GET_CAMPAIGN)
		{
			getCampaignFromCollection(mf, col, campaignId);
		}
		else if (action == GET_CAMPAIGNS)
		{
			getCampaignsFromCollection(mf, col);
		}
		else if (action == NEXT_ID)
		{
			getNextCampaignId(col);
		}
		else if (action == REBUILD)
		{
			removeExistingDocuments(col);
			for (int i = 0; i < numOfCampaigns; i++)
			{
				addCampaignToCollection(mf, col, numOfDocs, low, high);
				if (i % 10 == 0) { System.out.println("      on "+i); }
			}
		}
		else if (action == DELETE)
		{
			removeExistingDocuments(col);
		}
		
		System.out.println("number of documents in "+col.getName()+" after action:    "+col.count());
		System.out.println("end time: " + new java.util.Date());

		mf.close(db);
	}
	
	
	
	/**
	 * 
	 * @return
	 */
	public int getRandNumOfDocs(int low, int high)
	{
		int a = low;
		int b = high;
		double ran = Math.random();
		return (int)((b-a)*ran + a);
	}
	
	/**
	 * 
	 * @param col
	 * @return
	 */
	public List<?> getDistinctCampaignIds(DBCollection col)
	{
		return col.distinct("campaign_id");
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
	public int getNextCampaignId(DBCollection col)
	{
		List<?> l = getDistinctCampaignIds(col);
		int nextId = 0;
		for (int i = 0; i < l.size(); i++)
		{
			int id = Integer.parseInt(l.get(i).toString());
			if (nextId < id)
				nextId = id;
		}
		nextId++;
		return nextId;
	}
	
	/**
	 * 
	 * @param mf
	 * @param col
	 * @param numOfDocs
	 */
	public void addCampaignToCollection(MongoFactory mf, DBCollection col, int numOfDocs, int low, int high)
	{
		int campaign_id = getNextCampaignId(col);
		int starting_user_id = 1;

		if (numOfDocs == 0)
			numOfDocs = getRandNumOfDocs(low, high);
		
		for (int i = 1; i <= numOfDocs; i++)
		{
			BasicDBObject sad = new BasicDBObject();
			sad.put("campaign_id", campaign_id);
			sad.put("user_id", String.valueOf(starting_user_id + i));
			sad.put("sent_date", new java.util.Date());
			col.insert(sad);
			if (i % 100000 == 0) { System.out.println("on " + i); }
		}
	}
	
	/**
	 * 
	 * @param mf
	 * @param col
	 */
	public void getCampaignsFromCollection(MongoFactory mf, DBCollection col)
	{
		String todo ="";
		
		//todo = "both";
		//todo = "agg";
		todo = "distinct";
		
		if (todo.equals("agg") || todo.equals("both"))
		{
			DBObject fields = new BasicDBObject("campaign_id", 1);
			fields.put("_id", 0);
			DBObject project = new BasicDBObject("$project",fields);
			
			DBObject groupfields = new BasicDBObject("_id","$campaign_id");
			groupfields.put("number_of_sends", new BasicDBObject("$sum",1));
			DBObject group = new BasicDBObject("$group", groupfields);
			
			AggregationOutput output = col.aggregate(project, group);
		}
		else if (todo.equals("distinct") || todo.equals("both"))
		{
			List<?> l = col.distinct("campaign_id");
			
			System.out.println("number of distinct campaign ids : " + l.size());
			
			for (int i = 0; i < l.size(); i++)
			{
				System.out.println("campaign_id " + i + " has " + col.count(new BasicDBObject("campaign_id",i)));
			}
		}
	}
	
	/**
	 * 
	 * @param mf
	 * @param col
	 * @param campaignId
	 */
	public void getCampaignFromCollection(MongoFactory mf, DBCollection col, int campaignId)
	{
		DBObject query = new BasicDBObject("campaign_id", campaignId);
		DBCursor cur = col.find(query);
		System.out.println("count for campaign id: " + campaignId + " is " + cur.count());
		//while (cur.hasNext())
		//	System.out.println("   "+cur.next());
	}	
	
}
