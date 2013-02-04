<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
    
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core_rt" %>
<%@ taglib prefix="f" uri="http://java.sun.com/jstl/fmt" %>

<jsp:directive.page import="java.util.List" />
<jsp:directive.page import="the.app.admin.DBMgmt" />
<jsp:useBean id="DBMgmt" class="the.app.admin.DBMgmt" scope="session" />
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
        <link rel="stylesheet" type="text/css" href="css/basic.css" id="thecss">
        <title>TheApp :: DB Management</title>
        <script language="javascript">
        <!--
            function toggle(id) {
	            var e = document.getElementById(id);
	            if(e.style.display == 'none') {
	                e.style.display = 'block';
	            } else {
	                e.style.display = 'none';
	            }
	        }
        -->
        </script>
        <style type="text/css">#div1, #div2, #div3 {position:absolute; top: 100; left: 200; width:200; visibility:hidden}</style>        
    </head>
    <body>
        <center>
        <form action="db_management" method="post" name="searchform">
        <input type="hidden" id="a" name="a" value="setconnect"/>
        <input type="hidden" id="connect" name="connect" value="new"/>
        <table cellpadding="0" cellspacing="0" border="0" width="60%">
            <tr><td height="50" valign="middle" colspan="2">TheApp Project</td></tr>
            <tr><td height="50" valign="middle" colspan="2">Connect<small>&nbsp;&nbsp;(<a href="index.jsp">home</a>)</small></td></tr>
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
            <tr>
                <td height="30" valign="middle" width="30%">IP</td>
                <td height="30" valign="middle" width="70%"><input type="text" size="30" id="dblocation" name="dblocation" value="${DBMgmt.dbLocation}"/></td>
            </tr>
            <tr>                
                <td height="30" valign="middle" width="30%">Port</td>
                <td height="30" valign="middle" width="70%"><input type="text" size="30" id="dbport" name="dbport" value="${DBMgmt.dbPort}"/></td>
            </tr>
            <tr>                 
                <td height="30" valign="middle" width="30%">DB</td>
                <td height="30" valign="middle" width="70%"><input type="text" size="30" id="dbname" name="dbname" value="${DBMgmt.dbName}"/></td>
            </tr>
            <tr>                 
                <td height="30" valign="middle" width="30%">Login</td>
                <td height="30" valign="middle" width="70%"><input type="checkbox" id="dbusevalidation" name="dbusevalidation" /></td>
            </tr>
            <tr>                 
                <td height="30" valign="middle" width="30%">Username</td>
                <td height="30" valign="middle" width="70%"><input type="text" size="30" id="dbusername" name="dbusername" value="${DBMgmt.dbUsername}"/></td>
            </tr>
            <tr>                 
                <td height="30" valign="middle" width="30%">Password</td>
                <td height="30" valign="middle" width="70%"><input type="text" size="30" id="dbpassword" name="dbpassword" value="${DBMgmt.dbPassword}"/></td>
            </tr>
            <tr>                 
                <td height="30" valign="middle" width="30">Use to Auth</td>
                <td height="30" valign="middle" width="70%"><input type="checkbox" id="dbusetoauth" name="dbusetoauth" /></td>
            </tr>
            <tr><td height="50" valign="middle"  align="center" colspan="2"><input type="submit" value="Save Connect Info"/></td></tr>
        </table>
        </form>
        </center>
    </body>
</html>