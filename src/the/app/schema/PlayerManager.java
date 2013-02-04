package the.app.schema;

import com.google.code.morphia.Datastore;

public class PlayerManager extends  SchemaManager 
{

	private Schema schema;
	private String schemaName = "players";

	public PlayerManager(Datastore ds)
	{
		schema = getSchemaWithName(ds, schemaName);
	}
	
	
	
}
