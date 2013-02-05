package puzzle.populate;

import java.util.Vector;

public class Database 
{

	public Vector<Realm> realms;
	public Vector<Campaign> campaigns;
	public Vector<String> cities;
	public Vector<String> states;
	public Vector<Integer> events;
	private int realmMultiplier = 10;
	private int numOfRealmsToCreate = 1;
	
	public Database(int realmMultiplier, int numOfRealmsToCreate)
	{
		this.realmMultiplier = realmMultiplier;
		
		if (numOfRealmsToCreate > 0)
			this.numOfRealmsToCreate = numOfRealmsToCreate;
		
		initRealms();
		initData();
	}
	
	public void showRealms()
	{
		for (int i = 0; i < realms.size(); i++)
		{
			Realm r = realms.get(i);
			System.out.println("id: " + r.getId()+", name: " + r.getName()+", count: " + r.getSubscriberCount());
		}
	}
	
	public void showCampaigns()
	{
		for (int i = 0; i < campaigns.size(); i++)
		{
			Campaign c = campaigns.get(i);
			System.out.println("id: " + c.getId()+", realm_id: "+ c.getRealmId()+", name: " + c.getName());
		}
	}	
	
	
	
	private void initData()
	{
		if (cities == null || cities.size() == 0)
		{
			cities = new Vector<String>();
			cities.add("atlanta");
			cities.add("roswell");
			cities.add("alpharetta");
			cities.add("dallas");
			cities.add("san fransisco");
			cities.add("houston");
			cities.add("tempe");
			cities.add("jacksonville");
			cities.add("valdosta");
			cities.add("memphis");
		}
		
		if (states == null || states.size() == 0)
		{
			states = new Vector<String>();
			states.add("ga");
			states.add("ga");
			states.add("ga");
			states.add("tx");
			states.add("ca");
			states.add("tx");
			states.add("az");
			states.add("fl");
			states.add("ga");
			states.add("tn");
		}
		
		if (events == null || events.size() == 0)
		{
			events = new Vector<Integer>();
			events.add(new Integer(10));   // open
			events.add(new Integer(11));   // click
			events.add(new Integer(12));   // send
			events.add(new Integer(13));   // bounce
		}
	}
	
	private void initRealms()
	{
		if (realms == null || realms.size() == 0)
		{
			realms = new Vector<Realm>();
			for (int i = 1; i <= numOfRealmsToCreate; i++)
			{
				Realm r = new Realm();
				r.setId(i);
				r.setName("realm"+i);
				r.setSubscriberCount(i*realmMultiplier);
				realms.add(r);
			}
		}
		
		initCampaigns();
	}
	
	private void initCampaigns()
	{
		if (campaigns == null || campaigns.size() == 0)
		{
			int cid = 1;
			campaigns = new Vector<Campaign>();
			for (int j = 0; j < realms.size(); j++)
			{
				Realm r = realms.get(j);
				for (int i = 1; i <= 3; i++)
				{
					Campaign c = new Campaign();
					c.setId(cid);
					c.setRealmId(r.getId());
					c.setName(r.getId()+"campaign"+cid);
					campaigns.add(c);
					cid++;
				}				
			}

		}
	}	
	
	public class Realm
	{
		private int id;
		private String name;
		private int subscriberCount;
		
		public void setId(int id) { this.id = id; }
		public void setName(String name) { this.name = name; }
		public void setSubscriberCount(int subscriberCount) { this.subscriberCount = subscriberCount; }
		
		public int getId() { return this.id; }
		public String getName() { return this.name; }
		public int getSubscriberCount() { return this.subscriberCount; }
	}
	
	public class Campaign
	{
		private int id;
		private int realmId;
		private String name;
		
		public void setId(int id) { this.id = id; }
		public void setRealmId(int realmId) { this.realmId = realmId; }
		public void setName(String name) { this.name = name; }
		
		public int getId() { return this.id; }
		public int getRealmId() { return this.realmId; }
		public String getName() { return this.name; }
	}	
}
