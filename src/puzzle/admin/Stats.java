package puzzle.admin;

import java.util.Enumeration;
import java.util.Hashtable;


import com.mongodb.BasicDBObject;

public class Stats extends Bean 
{

	/**
	 * empty constructor
	 */
	public Stats() {}
	
	public Stats(BasicDBObject obj) 
	{
		convertBasicDBObjectToStats(obj);
	}

	// variables containing league collection information
	private String[] cols = {"serverUsed", "db", "collections", "objects", "avgObjSize", "dataSize",
			                 "storageSize", "numExtents", "indexes", "indexSize", "fileSize", "nsSizeMB", "ok"};
	private Hashtable<String, Object> data = new Hashtable<String, Object>();
	private String json;
	private String [] indexNames;
	
	// setters
	public void setJson(String s) 			{ json = s; }
	public void serverUsed(String s) 		{ data.put("serverUsed", s); }
    public void setDb(String s)				{ data.put("db", s); }
    public void setCollections(Integer i)	{ data.put("collections", i); }
    public void setObjects(Integer i)		{ data.put("objects", i); }
    public void setAvgObjSize(Double d)		{ data.put("avgObjSize", d); }
    public void setDataSize(Integer i)		{ data.put("dataSize", i); }
    public void setStorageSize(Integer i)	{ data.put("storageSize", i); }
    public void setNumExtents(Integer i)	{ data.put("numExtends", i); }
    public void setIndexes(Integer i)		{ data.put("indexes", i); }
    public void setIndexSize(Integer i)		{ data.put("indexSize", i); }
    public void setFileSize(Integer i)		{ data.put("fileSize", i); }
    public void setNsSizeMB(Integer i)		{ data.put("nsSizeMB", i); }
    public void setOk(Double d)				{ data.put("ok", d); }
    public void setIndexNames(String[] s)	{ indexNames = s; } 
	
	// getters
    public String getJson()					{ if (json == null) { json = ""; } return json; }
	public String getServerUsed() 			{ return (String)data.get("serverUsed"); }	
    public String getDb()					{ return (String)data.get("db"); }
    public Integer getCollections() 		{ return (Integer)data.get("collections"); }
    public Integer getObjects()				{ return (Integer)data.get("objects"); }
    public Double getAvgObjSize()			{ return (Double)data.get("avgObjSize"); }
    public Integer getDataSize()			{ return (Integer)data.get("dataSize"); }
    public Integer getStorageSize()			{ return (Integer)data.get("storageSize"); }
    public Integer getNumExtents()			{ return (Integer)data.get("numExtends"); }
    public Integer getIndexes()				{ return (Integer)data.get("indexes"); }
    public Integer getIndexSize()			{ return (Integer)data.get("indexSize"); }
    public Integer getFileSize()			{ return (Integer)data.get("fileSize"); }
    public Integer getNsSizeMB()			{ return (Integer)data.get("nsSizeMB"); }
    public Double getOk()					{ return (Double)data.get("ok"); }
    public String[] getIndexNames()			{ return indexNames; }
    
    /**
	 * creates a BasicDBObject from the data Hashtable
	 * @return BasicDBObject
	 */
	public BasicDBObject convertStatsToBasicDBObject()
	{
		BasicDBObject obj = new BasicDBObject();
		Enumeration<String> e = data.keys();
		while (e.hasMoreElements())
		{
			String key = e.nextElement();
			obj.put(key, String.valueOf(data.get(key)));
		}
		setJson(obj.toString());
		return obj;
	}
	
	/**
	 * creates a league object from a BasicDBObject
	 * @param obj
	 */
	public void convertBasicDBObjectToStats(BasicDBObject obj)
	{
		setJson(obj.toString());
		for (String s : cols)
		{
			data.put(s, obj.get(s));
		}
	}	
	
	/**
	 * the key, class of value, and value are shown for each row in the data hashtable
	 */
	public void showData()
	{
		Enumeration<String> e = data.keys();
		System.out.println(" ");
		System.out.println("-----------------------------------");
		System.out.println("stats.showMe()");
		while (e.hasMoreElements())
		{
			String k = e.nextElement();
			Object o = (Object)data.get(k);
			System.out.println("    key: " + k + ", class: " + o.getClass() + ", value: " + o);
		}

		System.out.println("indexes - count: " + indexNames.length);
		for (int i = 0; i < indexNames.length; i++)
		{
			System.out.println("    index: " + indexNames[i]);
		}
		System.out.println("-----------------------------------");
	}	
}

