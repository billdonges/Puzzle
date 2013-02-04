package the.app.schema;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;

@Entity
public class Column 
{

	public Column()
	{
	}
	
	public Column(ObjectId id, String dbName, String displayName)
	{
		this.setId(id);
		this.setDbName(dbName);
		this.setDisplayName(displayName);
	}
	
	@Id ObjectId id;
	private String dbName;
	private String displayName;
	
	/**
	 * @return the id
	 */
	public ObjectId getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(ObjectId id) {
		this.id = id;
	}
	/**
	 * @return the dbName
	 */
	public String getDbName() {
		return dbName;
	}
	/**
	 * @param dbName the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	/**
	 * @return the displayName
	 */
	public String getDisplayName() {
		return displayName;
	}
	/**
	 * @param displayName the displayName to set
	 */
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
}
