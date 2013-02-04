<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
		<link rel="stylesheet" type="text/css" href="css/basic.css" id="thecss">
		<title>TheApp Project</title>
	</head>
	<body>
        <center>
        <table width="70%">
            <tr><td height="50" valign="middle" colspan="2">TheApp Project - MongoDB Management</td></tr> 
            <tr>
                <td height="50" valign="middle" width="30%"><a href="db_connect.jsp">Connect</a></td>
                <td height="50" valign="middle" width="70%">Set DB Connection Info</td>
            </tr>        
            <tr>
                <td height="50" valign="middle" width="30%"><a href="db_management.jsp">Collections</a></td>
                <td height="50" valign="middle" width="70%">Select a DB and view it's collections and collection data</td>
            </tr>
            <tr>
                <td height="50" valign="middle" width="30%"><a href="db_management?a=alldbstats">Statistics</a></td>
                <td height="50" valign="middle" width="70%">View the size and health of client db's</td>
            </tr>            
        </table>
        </center>
	</body>
</html>