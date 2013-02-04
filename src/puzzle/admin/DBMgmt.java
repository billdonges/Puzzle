package puzzle.admin;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.DB;

/**
 * @author bdonges
 *
 */
public class DBMgmt 
{

	private String message;
	private int messageLength;
	private String errorMessage;
	private int errorMessageLength;
	private String dbLocation;
	private int dbPort;
	private boolean validate = false;
	private String validationChecked;
	private String dbUsername;
	private String dbPassword;
	private List<String> dbs;
	private String dbName;
	private String authDbName;
	private Stats stats;
	private List<String> cols;
	private String colName;
	private List<String> rows;
	private boolean useToAuth = false;
	private String useToAuthChecked;
	private DB authDB;	
	private List<Stats> allDbStats;
	private int pagenum;
	private int numtoget;
	private int rowcount;
	private String searchString;	

	public String getSearchString() {
		return searchString;
	}
	public void setSearchString(String searchString) {
		this.searchString = searchString;
	}
	public int getRowcount() {
		return rowcount;
	}
	public void setRowcount(int rowcount) {
		this.rowcount = rowcount;
	}
	public int getNumtoget() {
		return numtoget;
	}
	public void setNumtoget(int numtoget) {
		this.numtoget = numtoget;
	}
	public int getPagenum() {
		return pagenum;
	}
	public void setPagenum(int pagenum) {
		this.pagenum = pagenum;
	}
	/**
	 * @return the allDbStats
	 */
	public List<Stats> getAllDbStats() 
	{
		if (allDbStats == null)
			allDbStats = new ArrayList<Stats>();
		return allDbStats;
	}
	/**
	 * @param allDbStats the allDbStats to set
	 */
	public void setAllDbStats(List<Stats> allDbStats) {
		this.allDbStats = allDbStats;
	}
	/**
	 * @return the stats
	 */
	public Stats getStats() {
		if (stats == null)
			stats = new Stats();
		return stats;
	}
	/**
	 * @param stats the stats to set
	 */
	public void setStats(Stats stats) {
		this.stats = stats;
	}
	/**
	 * @return the authDbName
	 */
	public String getAuthDbName() {
		if (authDbName == null)
			authDbName = "";
		return authDbName;
	}
	/**
	 * @param authDbName the authDbName to set
	 */
	public void setAuthDbName(String authDbName) {
		this.authDbName = authDbName;
	}
	/**
	 * @return the useToAuthChecked
	 */
	public String getUseToAuthChecked() {
		return useToAuthChecked;
	}
	/**
	 * @param useToAuthChecked the useToAuthChecked to set
	 */
	public void setUseToAuthChecked(String useToAuthChecked) {
		this.useToAuthChecked = useToAuthChecked;
	}
	/**
	 * @return the validationChecked
	 */
	public String getValidationChecked() {
		return validationChecked;
	}
	/**
	 * @return the authDB
	 */
	public DB getAuthDB() {
		return authDB;
	}
	/**
	 * @param authDB the authDB to set
	 */
	public void setAuthDB(DB authDB) {
		this.authDB = authDB;
	}
	/**
	 * @return "checked" if useToAuth == true
	 */
	public String getUserToAuthChecked() {
		String s = "";
		if (isUseToAuth())
			s = "checked";
		return s;
	}
	/**
	 * @return the useToAuth
	 */
	public boolean isUseToAuth() {
		return useToAuth;
	}
	/**
	 * @param useToAuth the useToAuth to set
	 */
	public void setUseToAuth(boolean useToAuth) {
		this.useToAuth = useToAuth;
	}
	/**
	 * @param checked the checked to set
	 */
	public void setValidationChecked(String validationChecked) {
		this.validationChecked = validationChecked;
	}
	/**
	 * @return the dbLocation
	 */
	public String getDbLocation() {
		if (dbLocation == null) 
			dbLocation = "";
		return dbLocation;
	}
	/**
	 * @param dbLocation the dbLocation to set
	 */
	public void setDbLocation(String dbLocation) {
		this.dbLocation = dbLocation;
	}
	/**
	 * @return the dbPort
	 */
	public int getDbPort() {
		if (dbPort == 0) 
			dbPort = 27017;
		return dbPort;
	}
	/**
	 * @param dbPort the dbPort to set
	 */
	public void setDbPort(int dbPort) {
		this.dbPort = dbPort;
	}
	/**
	 * @return the validate
	 */
	public boolean isValidate() {
		return validate;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getChecked() {
		String s = "";
		if (validate)
			s = "checked";
		return s;
	}
	/**
	 * @param validate the validate to set
	 */
	public void setValidate(boolean validate) {
		this.validate = validate;
	}
	/**
	 * @return the dbUsername
	 */
	public String getDbUsername() {
		if (dbUsername == null)
			dbUsername = "";
		return dbUsername;
	}
	/**
	 * @param dbUsername the dbUsername to set
	 */
	public void setDbUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}
	/**
	 * @return the dbPassword
	 */
	public String getDbPassword() {
		if (dbPassword == null)
			dbPassword = "";
		return dbPassword;
	}
	/**
	 * @param dbPassword the dbPassword to set
	 */
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}
	/**
	 * @return the rows
	 */
	public List<String> getRows() {
		return rows;
	}
	/**
	 * @param rows the rows to set
	 */
	public void setRows(List<String> rows) {
		this.rows = rows;
	}
	/**
	 * @return the dbName
	 */
	public String getDbName() {
		if (dbName == null)
			dbName = "";
		return dbName;
	}
	/**
	 * @param dbName the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	/**
	 * @return the cols
	 */
	public List<String> getCols() {
		return cols;
	}
	/**
	 * @param cols the cols to set
	 */
	public void setCols(List<String> cols) {
		this.cols = cols;
	}
	/**
	 * @return the colName
	 */
	public String getColName() {
		if (colName == null)
			colName = "";
		return colName;
	}
	/**
	 * @param colName the colName to set
	 */
	public void setColName(String colName) {
		this.colName = colName;
	}
	/**
	 * @return the messageLength
	 */
	public int getMessageLength() {
		messageLength = getMessage().length();
		return messageLength;
	}
	/**
	 * @param messageLength the messageLength to set
	 */
	public void setMessageLength(int messageLength) {
		messageLength = getMessage().length();
		this.messageLength = messageLength;
	}
	/**
	 * @return the errorMessageLength
	 */
	public int getErrorMessageLength() {
		errorMessageLength = getErrorMessage().length();
		return errorMessageLength;
	}
	/**
	 * @param errorMessageLength the errorMessageLength to set
	 */
	public void setErrorMessageLength(int errorMessageLength) {
		errorMessageLength = getErrorMessage().length();
		this.errorMessageLength = errorMessageLength;
	}
	/**
	 * @return the dbs
	 */
	public List<String> getDbs() {
		return dbs;
	}
	/**
	 * @param dbs the dbs to set
	 */
	public void setDbs(List<String> dbs) {
		this.dbs = dbs;
	}
	/**
	 * @return the message
	 */
	public String getMessage() {
		if (message == null)
			message = "";
		return message;
	}
	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}
	/**
	 * @return the errorMessage
	 */
	public String getErrorMessage() {
		if (errorMessage == null)
			errorMessage = "";
		return errorMessage;
	}
	/**
	 * @param errorMessage the errorMessage to set
	 */
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
}
