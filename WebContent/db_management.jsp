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
        <title>TheApp :: MongoDB Management</title>
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
                <td height="30" valign="middle" width="15%" bgcolor="#C0C0C0">DBs</td>
                <td height="30" valign="middle" width="15%" bgcolor="#C0C0C0">Collections</td>
                <td height="30" valign="middle" width="70%" bgcolor="#C0C0C0">Data</td>
            </tr>
            <tr>
            	<td colspan="2" height="30">&nbsp;</td>
            	<td height="30" valign="middle" width="70%">
            		<input type="hidden" id="pagenum" name="pagenum" value="${DBMgmt.pagenum}"/>
            		<input type="hidden" id="numtoget" name="numtoget" value="${DBMgmt.numtoget}"/>
            		Page&nbsp;${DBMgmt.pagenum}&nbsp;Showing&nbsp;${DBMgmt.numtoget}&nbsp;of&nbsp;${DBMgmt.rowcount}
            		<a href="db_management?a=dta&colname=${DBMgmt.colName}&dbname=${DBMgmt.dbName}&pagenum=${DBMgmt.pagenum}&numtoget=${DBMgmt.numtoget}">Next</a>&nbsp;&nbsp;
            		<a href="db_management?a=colsearch&colname=${DBMgmt.colName}&dbname=${DBMgmt.dbName}&searchstring=${DBMgmt.searchString}">Search</a>
            	</td>
            </tr>
            <tr>
                <td height="30" valign="top" width="15%">
                <c:forEach var="db" items="${DBMgmt.dbs}">
                        <a href="db_management?a=col&dbname=${db}">${db}</a><c:if test="${DBMgmt.dbName == db}">*<c:set var="selected_db" value="${db}" scope="page" /></c:if><br/><br/>
                </c:forEach>
                </td>
                <c:choose>
                    <c:when test="${DBMgmt.dbName == selected_db}">
	                <td valign="top" width="15%">
                        <c:forEach var="col" items="${DBMgmt.cols}">
	                    <a href="db_management?a=dta&colname=${col}&dbname=${selected_db}">${col}</a><c:if test="${DBMgmt.colName == col}">*<c:set var="selected_col" value="${col}" scope="page" /></c:if><br/><br/>
	                    </c:forEach>
                        <c:if test="${DBMgmt.cols.size() == 0 }"><small><font color="red">no collections found</font></small></c:if>	                    
	               </td>
	               </c:when>
                    <c:otherwise>
                        <td colspan="2">&nbsp;</td>
                    </c:otherwise>	            
	            </c:choose>
                <td valign="top" width="70%">
	            <c:choose>
                    <c:when test="${DBMgmt.colName == selected_col}">
                        <c:forEach var="row" items="${DBMgmt.rows}">
	                    <small>${row}</small><br/>
	                    </c:forEach>
	                    <c:if test="${DBMgmt.rows.size() == 0 }"><small><font color="red">no data found</font></small></c:if>
                    </c:when>
                    <c:otherwise>
                        <td height="30" width="70%">&nbsp;</td>
                    </c:otherwise>
                </c:choose>
                </td>                        
            </tr>
        </table>
        
    </body>
</html>