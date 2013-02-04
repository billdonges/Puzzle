package the.app.set;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class Basic 
{

	public static void main(String[] args)
	{
		try
		{
			Basic b = new Basic();
			
			Set<Integer> big = b.genIntSet(1000000,100000);
			System.out.println("big.size: " + big.size());
			//b.printSet(b.sort(big));
			System.out.println(" ");
			
			Set<Integer> little = b.genIntSet(1000000,90000);
			System.out.println("little.size: " + little.size());
			//b.printSet(b.sort(little));
			System.out.println(" ");
			
			System.out.println("is little a subset of big? "+b.isSubset(big, little));
			System.out.println(" ");
			
			Set<Integer> union = b.union(big, little);
			System.out.println("union.  size: " + union.size());
			//b.printSet(b.sort(union));
			System.out.println(" ");			
			
			//System.out.println("compare big and little");
			//b.doesBigContainLittle(big, little);
			
			Set<Integer> intersect = b.intersect(big, little);
			System.out.println("intersect.  size: " + intersect.size());
			//b.printSet(b.sort(intersect));
			System.out.println(" ");			
			
			Set<Integer> d1 = b.difference(big, little);
			System.out.println("difference (big to little).  size: " + d1.size());
			Set<Integer> d2 = b.difference(little, big);
			System.out.println("difference (little to big).  size: " + d2.size());
			//b.printSet(b.sort(difference));
			System.out.println("difference (between d1 and d2):    " + (d1.size()-d2.size()));
			System.out.println(" ");
		}
		catch (Exception e)
		{
			System.err.println("exception - main - message: " + e.getMessage());
			e.printStackTrace();
		}
	}
	

	
	/**
	 * 
	 * @param big
	 * @param little
	 */
	public void doesBigContainLittle(Set<Integer> big, Set<Integer> little)
	{
		Iterator<Integer> it = little.iterator();
		while (it.hasNext())
		{
			int n = it.next();
			String msg = "  big does not contain "+n;
			if (big.contains(n))
			{
				msg = "  big does contain "+n;
			}
			System.out.println(msg);
		}
	}
	
	/**
	 * 
	 * @param set
	 */
	public void printSet(Set<Integer> set)
	{
		String s = "";
		Iterator<Integer> it = set.iterator();
		while (it.hasNext())
		{
			s = s + it.next() + ", ";
		}
		
		if (s.length() > 0)
		{
			s = s.substring(0, s.length()-2);
		}
		System.out.println("["+s+"]");
	}
	
	/**
	 * 
	 * @param s1
	 * @param s2
	 * @return
	 */
	public Set<Integer> intersect(Set<Integer> s1, Set<Integer>s2) 
	{
		Set<Integer> intersect = new HashSet<Integer>();
		Set<Integer> s = new HashSet<Integer>();
		Iterator<Integer> it = null;
		if (s1.size() >= s2.size())
		{
			it = s1.iterator();
			s = new HashSet<Integer>(s2);
		}
		else
		{
			it = s2.iterator();
			s = new HashSet<Integer>(s1);
		}

		while (it.hasNext())
		{
			int next = it.next();
			if (s.contains(next))
			{
				intersect.add(next);
			}
		}
		
		return intersect;
	}
	
	/**
	 * 
	 * @param s1
	 * @param s2
	 * @return
	 */
	public Set<Integer> difference(Set<Integer> numerator, Set<Integer>denominator) 
	{
		Set<Integer> difference = new HashSet<Integer>();
		Iterator<Integer> it1 = numerator.iterator();
		while (it1.hasNext())
		{
			int i = it1.next();
			if (!denominator.contains(i))
			{
				difference.add(i);
			}
		}
		return difference;
	}	
	
	/**
	 * 
	 * @param s1
	 * @param s2
	 * @return
	 */
	public boolean isSubset(Set<Integer> s1, Set<Integer> s2) 
	{
		boolean is = false;

		Set<Integer> union = union(s1, s2);
		
		int max = s1.size();
		if (s2.size() > s1.size())
		{
			max = s2.size();
		}
		
		if (union.size() == max)
		{
			is = true;
		}
		
		return is;
	}
	
	/**
	 * 
	 * @param s1
	 * @param s2
	 * @return
	 */
	public Set<Integer> union(Set<Integer> s1, Set<Integer> s2) 
	{
		Set<Integer> s3 = new HashSet<Integer>(s1);
		s3.addAll(s2);
		return s3;
	}	
	
	/**
	 * 
	 * @param randMax
	 * @param setSize
	 * @return
	 */
	public Set<Integer> genIntSet(int randMax, int setSize)
	{
		Set<Integer> s = new HashSet<Integer>();
		
		Random r = new Random();
		for (int i = 0; i < setSize; i++)
		{
			int n = r.nextInt(randMax);
			s.add(n);
		}
		
		return s;
	}
	
	/**
	 * 
	 * @param orig
	 * @return
	 */
	public Set<Integer> sort(Set<Integer> orig)
	{
		Set<Integer> sorted = new TreeSet<Integer>(orig);
		return sorted;
	}
	
}
