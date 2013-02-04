package the.app.schema;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.google.code.morphia.annotations.*;

@Entity
public class Version 
{

	public Version()
	{
	}
	
	public Version(ObjectId id, int version, int nextColumnNum, List<Column> columns)
	{
		this.setId(id);
		this.setVersion(version);
		this.setNextColumnNum(nextColumnNum);
		this.setColumns(columns);
	}
	
	// variables
	@Id private ObjectId id;
	private int version;
	private int nextColumnNum;
	List<Column> columns = new ArrayList<Column>();
	
	// setters
	public void setId(ObjectId id) { this.id = id; }
	public void setVersion(int version) { this.version = version; }
	public void setNextColumnNum(int nextColumNum) { this.nextColumnNum = nextColumNum; }
	public void setColumns(List<Column> columns) 
	{ 
		if (columns == null) 
			columns = new ArrayList<Column>(); 
		this.columns = columns; 
	}
	
	// getters
	public ObjectId getId() { return this.id; }
	public int getVersion() { return this.version; }
	public int getNextColumnNum() { return this.nextColumnNum; }
	public List<Column> getColumns() 
	{
		if (this.columns == null)
			this.columns = new ArrayList<Column>();
		return this.columns;
	}
	
	// other methods
	public int incrementNextColumnNum(int num)
	{
		this.setNextColumnNum(num);
		return this.getNextColumnNum();
	}
}
