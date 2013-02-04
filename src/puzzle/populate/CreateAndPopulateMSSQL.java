package puzzle.populate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import puzzle.populate.Database.Campaign;
import puzzle.populate.Database.Realm;

public class CreateAndPopulateMSSQL 
{

	public void checkTables(Connection mssql) throws Exception
	{
		Statement st = mssql.createStatement();
		
		PreparedStatement pst = mssql.prepareStatement("SELECT count(*) as cnt FROM sysobjects WHERE name = ?");
		pst.setString(1, "subscriber_test");
		ResultSet rs = pst.executeQuery();
		while (rs.next())
		{
			int cnt = rs.getInt("cnt");
			System.out.println("subscribers_test count: " + cnt);
			if (cnt == 0)
			{
				String sql = "CREATE TABLE [dbo].[subscriber_test]( "+
						"[id] [int] IDENTITY(1,1) NOT NULL, "+
						"[realm_id] [int] NOT NULL, "+
						"[email] [varchar](250) NULL, "+
						"[first_name] [varchar](20) NULL, "+ 
						"[last_name] [varchar](20) NULL, "+
						"[city] [varchar](30) NULL, "+ 
						"[state] [varchar](30) NULL, "+
					 "CONSTRAINT [PK_subscriber_test] PRIMARY KEY CLUSTERED "+ 
					 "( "+
						"[id] ASC "+
					 ")WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY] "+
					 ") ON [PRIMARY]";
				st.execute(sql);
			}
		}
		
		pst.clearParameters();
		
		pst.setString(1, "tracking_test");
		rs = pst.executeQuery();
		while (rs.next())
		{
			int cnt = rs.getInt("cnt");
			System.out.println("tracking_test count: " + cnt);
			if (cnt == 0)
			{
				String sql = "CREATE TABLE [dbo].[tracking_test]( "+
						"[id] [int] IDENTITY(1,1) NOT NULL, "+
						"[realm_id] [int] NOT NULL, "+
						"[subscriber_id] [int] NOT NULL, "+
						"[campaign_id] [int] NOT NULL, "+ 
						"[event_date] [datetime] NOT NULL," +
						"[event_type] [int] NOT NULL, "+
					 "CONSTRAINT [PK_tracking_test] PRIMARY KEY CLUSTERED "+ 
					 "( "+
						"[id] ASC "+
					 ")WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY] "+
					 ") ON [PRIMARY]";
				st.execute(sql);
			}
		}
		
		pst.close();
		st.close();
		rs.close();
	}
	
	public void insertSubscribers(Connection mssql, Database db) throws Exception
	{

		String query = "INSERT INTO subscriber_test (realm_id, email, first_name, last_name, city, state) VALUES (?,?,?,?,?,?)";
		PreparedStatement pst = mssql.prepareStatement(query);
		for (int i = 0; i < db.realms.size(); i++)
		{
			Realm r = (Realm)db.realms.get(i);
			System.out.println("    starting subscriber_test on realm "+r.getId()+ " at "+new java.util.Date());
			int cs = 0;
			for (int j = 0; j < r.getSubscriberCount(); j++)
			{
				
				pst.setInt(1, r.getId());
				pst.setString(2, "r"+r.getId()+"e"+j+"@bademailaddress.com");
				pst.setString(3, "first"+r.getId()+""+j);
				pst.setString(4, "last"+r.getId()+""+j);
				pst.setString(5, db.cities.get(cs));
				pst.setString(6, db.states.get(cs));

				/*
				System.out.println("    subscriber insert: ("+r.getId()+", "+"r"+r.getId()+"e"+j+"@bademailaddress.com"+", "+"first"+r.getId()+""+j+", "
						+"last"+r.getId()+""+j+", "+db.cities.get(cs)+", "+db.states.get(cs)+")");
				*/
				
				pst.execute();
				
				if (cs < 9)
				{
					cs++;
				}
				else if (cs == 9)
				{
					cs = 0;
				}
			}
			System.out.println("    stopping subscribers_test on realm "+r.getId()+ " at "+new java.util.Date());
			System.out.println(" ");
		}
		
		pst.close();
		pst = null;
	}
	
	public void insertTracking(Connection mssql, Database db) throws Exception
	{

		String insert = "INSERT INTO tracking_test (event_type, subscriber_id, event_date, realm_id, campaign_id) VALUES (?,?,getDate(),?,?)";
		PreparedStatement insertPst = mssql.prepareStatement(insert);
		
		String get = "SELECT id FROM subscriber_test WHERE realm_id = ?";
		PreparedStatement getPst = mssql.prepareStatement(get);
		
		
		for (int i = 0; i < db.realms.size(); i++)
		{
			Realm r = (Realm)db.realms.get(i);
			System.out.println("    starting tracking_test on realm "+r.getId()+ " at "+new java.util.Date());

			getPst.setInt(1, r.getId());
			ResultSet rs = getPst.executeQuery();
			while(rs.next())
			{
				int sid = rs.getInt("id");
				
				for (int ii = 0; ii < db.campaigns.size(); ii++)
				{
					Campaign c = db.campaigns.get(ii);
					if (c.getRealmId() == r.getId())
					{
						for (int iii = 0; iii < db.events.size(); iii++)
						{
							insertPst.setInt(1, db.events.get(iii));
							insertPst.setInt(2, sid);
							insertPst.setInt(3, r.getId());
							insertPst.setInt(4, c.getId());
							insertPst.execute();
							//System.out.println("    tracking insert: "+db.events.get(iii)+","+sid+","+r.getId());
						}
					}
				}
			}
			
			System.out.println("    stopping tracking_test on realm "+r.getId()+ " at "+new java.util.Date());
			System.out.println(" ");
			rs.close();
			rs = null;
		}
		insertPst.close();
		insertPst = null;
		
		getPst.close();
		getPst = null;
	}

	public void clean(Connection mssql) throws Exception
	{
		Statement st = mssql.createStatement();
		st.execute("DELETE FROM tracking_test WHERE id > 0");
		st.execute("DELETE FROM subscriber_test WHERE id > 0");
		st.close();
		st = null;
	}
}
