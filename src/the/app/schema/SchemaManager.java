package the.app.schema;

import java.util.List;

import org.bson.types.ObjectId;

import com.google.code.morphia.Datastore;

public class SchemaManager 
{

	/**
	 * saves a list of Schema objects
	 * @param Datastore
	 * @param List<Schema>
	 */
	public void saveSchemas(Datastore ds, List<Schema> schemas)
	{
		for (Schema schema : schemas)
		{
			saveSchema(ds, schema);
		}
	}
	
	/**
	 * saves a single Schema
	 * @param Datastore
	 * @param Schema
	 */
	public void saveSchema(Datastore ds, Schema schema)
	{
		ds.save(schema);
	}
	
	/**
	 * returns a single Schema using the schema name 
	 * @param Datastore
	 * @param String
	 * @return Schema
	 */
	public Schema getSchemaWithName(Datastore ds, String name)
	{
		return ds.find(Schema.class).filter("name", name).get();
	}
	
	/**
	 * returns a single Schema using the ObjectId
	 * @param ds
	 * @param id
	 * @return
	 */
	public Schema getSchemaWithId(Datastore ds, ObjectId id)
	{
		return ds.find(Schema.class).filter("id", id).get();
	}
	
	/**
	 * returns a list of all Schemas
	 * @param Datastore
	 * @return List<Schema>
	 */
	public List<Schema> getSchemas(Datastore ds)
	{
		return ds.find(Schema.class).asList();
	}
	
	/**
	 * removes a single Schema using an ObjectId
	 * @param Datastore
	 * @param Schema
	 */
	public void removeSchema(Datastore ds, Schema s)
	{
		ds.delete(ds.createQuery(Schema.class).filter("id", s.getId()));
	}
	
	/**
	 * removes all Schemas
	 * @param Datastore
	 */
	public void removeSchemas(Datastore ds)
	{
		ds.delete(ds.createQuery(Schema.class));
	}	
	
}
