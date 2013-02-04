package the.app;

public class AppValidation 
{

	/**
	 * 
	 * @param s
	 * @return
	 */
	public static String checkNull(String s)
	{
		if (s == null)
		{
			s = "";
		}
		return s;
	}
	
}
