<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
    
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core_rt" %>
<%@ taglib prefix="f" uri="http://java.sun.com/jstl/fmt" %>

<jsp:directive.page import="java.util.List" />
<jsp:directive.page import="the.app.admin.DBMgmt" />
<jsp:directive.page import="the.app.admin.Stats" />
<jsp:useBean id="DBMgmt" class="the.app.admin.DBMgmt" scope="session" />
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
        <link rel="stylesheet" type="text/css" href="css/basic.css" id="thecss">
        <title>TheApp :: MongoDB Search</title>
        <script language="javascript">
        <!--
            function doSearch() 
            {
           	 	var where = "db_management?a=colsearch&colname=${DBMgmt.colName}&dbname=${DBMgmt.dbName}&searchstring="+document.getElementById("searchstring").value;
	            document.location.href = where;
	        }
        -->
        </script>
        <style type="text/css">#div1, #div2, #div3 {position:absolute; top: 100; left: 200; width:200; visibility:hidden}</style>        
    </head>
    <body>
        <table cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr><td height="50" valign="middle" colspan="2">TheApp Project</td></tr>
            <tr><td height="50" valign="middle" colspan="2">Collections<small>&nbsp;&nbsp;(<a href="index.jsp">home</a>)</small></td></tr>
            <tr>
                <td height="50" valign="middle" colspan="2">
                    <c:if test="${DBMgmt.messageLength > 0}">
                        ${DBMgmt.message}<br/>
                    </c:if>
                    <c:if test="${DBMgmt.errorMessageLength > 0}">
                        ${DBMgmt.errorMessage}<br/>
                    </c:if>                  
                </td>
            </tr>
            <tr><td height="50" valign="middle" colspan="2">actions:&nbsp;&nbsp;<a href="db_management?a=dbs">get dbs</a></td></tr>
            <tr>
                <td height="30" valign="middle" width="15%" bgcolor="#C0C0C0">DB</td>
                <td height="30" valign="middle" width="15%" bgcolor="#C0C0C0">Collection</td>
                <td height="30" valign="middle" width="70%" bgcolor="#C0C0C0">Data</td>
            </tr>
            <tr>
            	<td height="30" valign="top" width="15%"><a href="db_management?a=col&dbname=${DBMgmt.dbName}">${DBMgmt.dbName}</a>*</td>
                <td height="30" valign="top" width="15%"><a href="db_management?a=dta&colname=${DBMgmt.colName}&dbname=${DBMgmt.dbName}">${DBMgmt.colName}</a>*</td>
            	<td height="30" valign="middle" width="70%">Search String:&nbsp;
            		<input type="text" size="30" id="searchstring" name="searchstring" value='${DBMgmt.searchString}'>
            			<a href="javascript:doSearch()">Search</a>
            		</input>
            	</td>
            </tr>
            <tr>
                <td colspan="2" height="30">&nbsp;</td>
                <td height="30" valign="middle" width="70%">Example:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{"key":"value"}
            	</td>                        
            </tr>
            <tr>
                <td colspan="2" height="30">&nbsp;</td>
                <td height="30" width="70%">
                    <c:forEach var="row" items="${DBMgmt.rows}">
                    <small>${row}</small><br/>
                    </c:forEach>
                    <c:if test="${DBMgmt.rows.size() == 0 }"><small><font color="red">no data found</font></small></c:if>
                </td>                        
            </tr>
        </table>
        
    </body>
</html>