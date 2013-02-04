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
        <title>TheApp :: MongoDB Statistics</title>
        <style type="text/css">#div1, #div2, #div3 {position:absolute; top: 100; left: 200; width:200; visibility:hidden}</style>        
    </head>
    <body>
        <table cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr><td height="50" valign="middle" colspan="8">TheApp Project</td></tr>
            <tr><td height="50" valign="middle" colspan="8">Statistics<small>&nbsp;&nbsp;(<a href="index.jsp">home</a>)</small></td></tr>
            <tr>
                <td height="50" valign="middle" colspan="8">
                    <c:if test="${DBMgmt.messageLength > 0}">
                        ${DBMgmt.message}<br/>
                    </c:if>
                    <c:if test="${DBMgmt.errorMessageLength > 0}">
                        ${DBMgmt.errorMessage}<br/>
                    </c:if>                  
                </td>
            </tr>
            <tr>
                <td height="30" valign="middle" width="15%" bgcolor="#C0C0C0">DB</td>
                <td height="30" valign="middle" width="85%" bgcolor="#C0C0C0" colspan="7">Statistics</td>
            </tr>
            <tr>
                <td height="30" valign="middle" width="15%">&nbsp;</td>
                <td height="30" valign="middle" width="20%">Server</td>
                <td height="30" valign="middle" width="10%">Collections</td>
                <td height="30" valign="middle" width="10%">Objects</td>
                <td height="30" valign="middle" width="10%">Data</td>
                <td height="30" valign="middle" width="15%">Storage</td>
                <td height="30" valign="middle" width="10%">Indexes</td>
                <td height="30" valign="middle" width="10%">File Size</td>
            </tr>
            <tr><td colspan="8" height="10">&nbsp;</td></tr>
            <c:forEach var="stat" items="${DBMgmt.allDbStats}">
            <tr>
                <td height="30" valign="top" width="15%">
                	<a href="db_management?a=col&dbname=${stat.getDb()}">${stat.getDb()}</a>
                </td>
                <td height="30" valign="top" width="20%">${stat.getServerUsed()}</td>
                <td height="30" valign="top" width="10%"><a href="db_management?a=dta&colname=${stat.getCollections()}&dbname=${stat.getDb()}">${stat.getCollections()}</a></td>
                <td height="30" valign="top" width="10%">${stat.getObjects()}</td>
                <td height="30" valign="top" width="10%">${stat.getDataSize()}</td>
                <td height="30" valign="top" width="15%">${stat.getStorageSize()}</td>
                <td height="30" valign="top" width="10%">${stat.getIndexes()}</td>
                <td height="30" valign="top" width="10%">${stat.getFileSize()}</td>
            </tr>
            </c:forEach>
        </table>
        
    </body>
</html>