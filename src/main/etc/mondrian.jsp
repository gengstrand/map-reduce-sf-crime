<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<jp:mondrianQuery id="query01" jdbcDriver="com.mysql.jdbc.Driver" jdbcUrl="jdbc:mysql://localhost/sfcrime" catalogUri="/WEB-INF/queries/sfcrime.xml" connectionPooling="false" jdbcUser="dbuser" jdbcPassword="dbpassword">
select {[Measures].[crimes]} ON COLUMNS,
  {([District].[All Districts], [Category].[All Categories])} ON ROWS
from [sfcrime]
where [Time].[2013]

</jp:mondrianQuery>





<c:set var="title01" scope="session">SF Crime Analytics</c:set>
