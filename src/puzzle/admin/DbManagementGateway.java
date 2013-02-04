package puzzle.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import puzzle.AppInfo;
import puzzle.AppValidation;

public class DbManagementGateway extends HttpServlet implements AppInfo
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res)throws ServletException, IOException 
	{       
		service(req,res);
	}

	/**
	 * 
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res)throws ServletException, IOException 
	{       
		service(req,res);
	}
	
	/**
	 * @throws IOException 
	 * @throws ServletException 
	 * 
	 */
	protected void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException 
	{
		System.out.println("DBManagementGateway.service()");
		
	    Enumeration<String> headerNames = req.getHeaderNames();
	    while(headerNames.hasMoreElements()) {
	        String headerName = (String)headerNames.nextElement();
	        String headerVal  = (String)req.getHeader(headerName);
	        System.out.println("name: " + headerName + ", val: " + headerVal);
	    }
		
		DBMgmt dbm = new DBMgmt();
		HttpSession session = req.getSession();
		if (session.getAttribute("DBMgmt") != null)
		{
			dbm = (DBMgmt)session.getAttribute("DBMgmt");
			dbm.setNumtoget(0);
			dbm.setPagenum(0);
			dbm.setRowcount(0);
			dbm.setSearchString("");
		}
		
		String url = "/db_management.jsp";
		
		try
		{
			/*
			 * possible actions
			 * 
			 * dbs = get dbs
			 * col = get collections in db
			 * dta = get data in specific collection
			 * dbstats = get db statistics
			 */
			String action = req.getParameter("a");
			action = AppValidation.checkNull(action);
			
			/*
			 * special cases.
			 * 
			 * connect = new when someone resubmits the the connect form.
			 */
			String connect = req.getParameter("connect");
			connect = AppValidation.checkNull(connect);
			
			System.out.println("action: " + action + ", connect: " + connect);

			

			DbManagementAction dba = new DbManagementAction(dbm);
			if (action.equals("setconnect")) 
			{
				if (connect.equals("new")) 
				{
					String dbLocation = AppValidation.checkNull(req.getParameter("dblocation"));
					if (dbLocation.equals(""))
						dbLocation = DB_LOCATION;
					
					String dbPortStr = AppValidation.checkNull(req.getParameter("dbport"));
					if (dbPortStr.equals(""))
						dbPortStr = "27017";
					
					int dbPort = Integer.parseInt(dbPortStr);
					if (dbPort == 0)
						dbPort = DB_PORT;
					
					String dbName = AppValidation.checkNull(req.getParameter("dbname"));
					if (dbName.equals(""))
						dbName = "test";
							
					String username = AppValidation.checkNull(req.getParameter("dbusername"));
					String password = AppValidation.checkNull(req.getParameter("dbpassword"));
					
					boolean useValidation = false;
					String useValidationStr = AppValidation.checkNull(req.getParameter("dbusevalidation"));
					if (useValidationStr.equals("on"))
						useValidation = true;
		
					boolean useToAuth = false;
					String useToAuthStr = AppValidation.checkNull(req.getParameter("dbusetoauth"));
					if (useToAuthStr.equals("on"))
						useToAuth = true;
					
					dbm.setDbLocation(dbLocation);
					dbm.setDbPort(dbPort);
					dbm.setDbName(dbName);
					dbm.setAuthDbName(dbName);
					dbm.setDbUsername(username);
					dbm.setDbPassword(password);
					dbm.setValidate(useValidation);
					dbm.setUseToAuth(useToAuth);
				}
				else 
				{
					if (!dbm.isUseToAuth()) 
					{
						if (dbm.getDbLocation().equals(""))
							dbm.setDbLocation(DB_LOCATION);
						
						if (dbm.getDbPort() == 0)
							dbm.setDbPort(DB_PORT);
						
						if (dbm.getDbName().equals(""))
							dbm.setDbName(DB_NAME);
						
						if (dbm.getAuthDbName().equals(""))
							dbm.setAuthDbName(DB_NAME);
						
						if (dbm.getChecked().equals("checked"))
							dbm.setValidate(true);
						
						dbm.setUseToAuth(false);
					}
				}
				
				url = "/db_connect.jsp";
			}
			else if (action.equals("dbs")) 
			{
				dbm.setDbs(dba.getDbs());
				dbm.setCols(new ArrayList<String>());
				dbm.setStats(new Stats());
				dbm.setRows(new ArrayList<String>());
			}
			else if (action.equals("col"))
			{
				String dbName = AppValidation.checkNull(req.getParameter("dbname"));
				dbm.setDbName(dbName);
				dbm.setDbs(dba.getDbs());
				dbm.setCols(dba.getCols(dbName));
				dbm.setStats(dba.getDbStats(dbName));
				dbm.setRows(new ArrayList<String>());
			}
			else if (action.equals("dta"))
			{
				dbm.setNumtoget(0);
				dbm.setPagenum(0);
				dbm.setRowcount(0);
				String dbName = AppValidation.checkNull(req.getParameter("dbname"));
				String colName = AppValidation.checkNull(req.getParameter("colname"));
				String pagenumstr = AppValidation.checkNull(req.getParameter("pagenum"));
				if (pagenumstr.length() == 0)
				{
					pagenumstr = "0";
				}
				String numtogetstr = AppValidation.checkNull(req.getParameter("numtoget"));
				if (numtogetstr.length() == 0)
				{
					numtogetstr = "25";
				}
				dbm.setNumtoget(Integer.parseInt(numtogetstr));
				dbm.setPagenum(Integer.parseInt(pagenumstr)+1);
				dbm.setDbName(dbName);
				dbm.setColName(colName);
				dbm.setDbs(dba.getDbs());
				dbm.setCols(dba.getCols(dbName));
				dbm.setRows(dba.getRows(dbName, colName, dbm.getPagenum(), dbm.getNumtoget()));
				dbm.setRowcount(dba.getRowCount(dbName, colName));
				if (dbm.getNumtoget() > dbm.getRowcount())
				{
					dbm.setNumtoget(dbm.getRowcount());
				}
			}
			else if (action.equals("alldbstats"))
			{
				dbm.setAllDbStats(dba.getDbStats());
				url = "/db_stats.jsp";
			}
			else if (action.equals("colsearch"))
			{
				String dbName = AppValidation.checkNull(req.getParameter("dbname"));
				String colName = AppValidation.checkNull(req.getParameter("colname"));
				String searchString = AppValidation.checkNull(req.getParameter("searchstring"));
				dbm.setSearchString(searchString);
				dbm.setRows(dba.getSearchRows(dbName, colName, searchString));
				url = "/col_search.jsp";
			}
			
			dbm.setMessage("Connected: "+dbm.getDbLocation()+":"+dbm.getDbPort()+"/"+dbm.getDbName());
			dbm.setErrorMessage("");							
		}
		catch (Exception e)
		{
			System.err.println("exception - dbmanagementgateway - message: " + e.getMessage());
			e.printStackTrace();
			if (e.getMessage().startsWith("cannot connect to mongo database at ")) 
			{
				dbm.setMessage("");
				dbm.setErrorMessage("you've visited the DBManagementGatway and had a problem...<br/><br/><font color=\"red\">"+e.getMessage()+"</font>");
			}
			else 
			{
				dbm.setMessage("");
				dbm.setErrorMessage("<font color=\"red\">"+e.getMessage()+"</font>");				
				e.printStackTrace();
			}
		}
		
		session.setAttribute("DBMgmt", dbm);
		
		RequestDispatcher rd = getServletContext().getRequestDispatcher(url);		
		rd.forward(req, res);
	}
	
	
}
