package the.app.schema;

import java.util.ArrayList;
import java.util.List;
import com.google.code.morphia.annotations.*;
import org.bson.types.ObjectId;

@Entity
public class Schema 
{

	public Schema()
	{
	}
	
	public Schema(ObjectId id, String name, List<Column> columns)
	{
		this.setId(id);
		this.setName(name);
		this.setColumns(columns);
	}
	
	@Id private ObjectId id;
	private String name;
	List<Column> columns = new ArrayList<Column>();
	@NotSaved private int columnCnt;
	
	// setters
	public void setId(ObjectId id) { this.id = id; }
	public void setName(String name) { this.name = name; }
	public void setColumns(List<Column> columns) 
	{ 
		if (columns == null)
		{
			columns = new ArrayList<Column>();
		}
		this.columns = columns; 
	}

	// getters
	public ObjectId getId() { return this.id; }
	public String getName() { return this.name; }
	public List<Column> getColumns() 
	{
		if (this.columns == null)
		{
			this.columns = new ArrayList<Column>();
		}
		return this.columns; 
	}
	
	public int getColumnCount()
	{
		this.columnCnt = 0;
		if (getColumns() != null && getColumns().size() > 0)
		{
			columnCnt = getColumns().size();
		}
		return this.columnCnt;
	}
}
