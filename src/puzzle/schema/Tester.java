package puzzle.schema;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import puzzle.db.mongo.MongoFactory;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.Mongo;

public class Tester 
{

	public static int SCHEMA = 1;
	
	public static void main(String[] args)
	{
		try
		{
			int toTest = Tester.SCHEMA;

			String dbLocation = "127.0.0.1";
			String dbPort = "27017";
			String dbName = "test";

			// get datastore
			Datastore ds = new MongoFactory().getDatastore(dbLocation, Integer.parseInt(dbPort), dbName);
			
			if (toTest == Tester.SCHEMA)
			{
				Tester.testSchema(ds);
			}
		}
		catch (Exception e)
		{
			System.err.println("exception - message: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static void testSchema(Datastore ds) throws Exception
	{
		System.out.println("testSchema()");
		
		List<Column> columns = new ArrayList<Column>();
		columns.add(new Column(new ObjectId(), "c1", "Player ID"));
		columns.add(new Column(new ObjectId(), "c2", "Name"));
		columns.add(new Column(new ObjectId(), "c3", "Position"));
		columns.add(new Column(new ObjectId(), "c4", "City"));
		columns.add(new Column(new ObjectId(), "c5", "Team"));
		
		List<Version> versions = new ArrayList<Version>();
		versions.add(new Version(new ObjectId(), 1, columns.size()+1, columns));
		
		Schema schema = new Schema(new ObjectId(), "players", columns);
		
		System.out.println("  save");
		ds.save(schema);
		
		List<Schema> list = ds.find(Schema.class).asList();
		for (Schema s : list)
		{
			System.out.println("    id: " +s.getId());
			System.out.println("    name: " +s.getName());
			for (Column c : s.getColumns())
			{
				System.out.println("        id: " +c.getId());
				System.out.println("        dbName: " +c.getDbName());
				System.out.println("        displayName: " +c.getDisplayName());
			}
		}
	}
	
}
