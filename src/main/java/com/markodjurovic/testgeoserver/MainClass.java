/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.markodjurovic.testgeoserver;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import sun.net.www.protocol.http.HttpURLConnection;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Idea is to insert some data into orientDB and to execute some queries via
 * geoserver WFS. Features testing is taken from geoserver demo
 *
 * @author mdjurovi
 */
public class MainClass {

  private final static String USER_AGENT = "TestApp";
  
  private static final String bboxBody = "<!-- Performs a get feature with a bounding box filter.      -->\n"
          + "<!-- The BBOX filter is a convenience for a <Not><Disjoint>, -->\n"
          + "<!-- it fetches all features that spatially interact with the given box. -->\n"
          + "<!-- This example also shows how to request specific properties, in this -->\n"
          + "<!-- case we just get the STATE_NAME and PERSONS -->\n"
          + "\n"
          + "<wfs:GetFeature service=\"WFS\" version=\"1.0.0\"\n"
          + "  outputFormat=\"GML2\"\n"
          + "  xmlns:topp=\"http://www.openplans.org/topp\"\n"
          + "  xmlns:wfs=\"http://www.opengis.net/wfs\"\n"
          + "  xmlns:ogc=\"http://www.opengis.net/ogc\"\n"
          + "  xmlns:gml=\"http://www.opengis.net/gml\"\n"
          + "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
          + "  xsi:schemaLocation=\"http://www.opengis.net/wfs\n"
          + "                      http://schemas.opengis.net/wfs/1.0.0/WFS-basic.xsd\">\n"
          + "  <wfs:Query typeName=\"test:Restaurant\">\n"
          + "    <ogc:Filter>\n"
          + "      <ogc:BBOX>\n"
          + "        <ogc:PropertyName>location</ogc:PropertyName>\n"
          + "        <gml:Box srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\">\n"
          + "           <gml:coordinates>-75.102613,-40.212597 72.361859,41.512517</gml:coordinates>\n"
          + "        </gml:Box>\n"
          + "      </ogc:BBOX>\n"
          + "   </ogc:Filter>\n"
          + "  </wfs:Query>\n"
          + "</wfs:GetFeature>";
  
  private static final String intersectsBody ="<!-- Performs an intersects against a point.  This is functionally -->\n" +
"<!-- equivalent to <Not><Disjoint>.  This call can be used by a    -->\n" +
"<!-- client application to select a feature clicked on.  \n" +
"\n" +
"     This will search through the dataset and return any polygons that\n" +
"     contain the search point.  \n" +
"     \n" +
"     If you were searching in a point or line dataset, you might want\n" +
"     to make a little polygon to search with instead of a single point\n" +
"     so the user doesnt have to *exactly* click on the (mathematically\n" +
"     infinitely thin) line or point.     \n" +
"\n" +
" -->\n" +
"<wfs:GetFeature service=\"WFS\" version=\"1.0.0\"\n" +
"  outputFormat=\"GML2\"\n" +
"  xmlns:topp=\"http://www.openplans.org/topp\"\n" +
"  xmlns:wfs=\"http://www.opengis.net/wfs\"\n" +
"  xmlns=\"http://www.opengis.net/ogc\"\n" +
"  xmlns:gml=\"http://www.opengis.net/gml\"\n" +
"  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
"  xsi:schemaLocation=\"http://www.opengis.net/wfs\n" +
"                      http://schemas.opengis.net/wfs/1.0.0/WFS-basic.xsd\">\n" +
"  <wfs:Query typeName=\"test:Restaurant\">\n" +
"    <Filter>\n" +
"      <Intersects>\n" +
"        <PropertyName>the_geom</PropertyName>\n" +
"          <gml:Point srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\">\n" +
"            <gml:coordinates>20,20</gml:coordinates>\n" +
"          </gml:Point>\n" +
"        </Intersects>\n" +
"      </Filter>\n" +
"  </wfs:Query>\n" +
"</wfs:GetFeature>";
  
  //this one should return same result as intersect
  private static final String notDisjointBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
"<!--\n" +
"    This example demonstrates a WFS 2.0 GetFeature POST request.\n" +
"\n" +
"    WFS 2.0 does not depend on any one GML version and thus\n" +
"    requires an explicit namespace and schemaLocation for GML.\n" +
"\n" +
"    This spatial filter selects a single feature with\n" +
"    gml:id=\"bugsites.2\".\n" +
"\n" +
"    See also:\n" +
"    WFS Standard: http://www.opengeospatial.org/standards/wfs\n" +
"    Filter Encoding Standard: http://www.opengeospatial.org/standards/filter\n" +
"-->\n" +
"<wfs:GetFeature service=\"WFS\" version=\"2.0.0\"\n" +
"    xmlns:wfs=\"http://www.opengis.net/wfs/2.0\" xmlns:fes=\"http://www.opengis.net/fes/2.0\"\n" +
"    xmlns:gml=\"http://www.opengis.net/gml/3.2\" xmlns:sf=\"http://www.openplans.org/spearfish\"\n" +
"    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
"    xsi:schemaLocation=\"http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd\n" +
"        http://www.opengis.net/gml/3.2 http://schemas.opengis.net/gml/3.2.1/gml.xsd\">\n" +
"    <wfs:Query typeNames=\"test:Restaurant\">\n" +
"        <fes:Filter>\n" +
"            <fes:Not>\n" +
"                <fes:Disjoint>\n" +
"                    <fes:ValueReference>sf:the_geom</fes:ValueReference>\n" +
"                    <!-- gml:id is mandatory on GML 3.2 geometry elements -->\n" +
"                    <gml:Point gml:id=\"p21\" srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\">\n" +
"                       <gml:coordinates>20, 20</gml:coordinates>\n" +
"                    </gml:Point>\n" +
"                </fes:Disjoint>\n" +
"            </fes:Not>\n" +
"        </fes:Filter>\n" +
"    </wfs:Query>\n" +
"</wfs:GetFeature>";

  public static void main(String[] args) {
    String storageClassName = "Restaurant";
    String fieldName = "location";
    String database = "demodb";
    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");
    String geoServerUrl = "http://localhost:8080/geoserver";
    geoServerUrl = "http://localhost:8080/geoserver/wfs";

    try {
      Connection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:remote:localhost/" + database, info);
      Statement statement = conn.createStatement();
      statement.executeUpdate("INSERT INTO " + storageClassName + "(" + fieldName + ") VALUES (St_GeomFromText(\"LINESTRING (30 10, 10 30, 40 40)\"))");
      statement.executeUpdate("INSERT INTO " + storageClassName + "(" + fieldName + ") VALUES (St_GeomFromText(\"GEOMETRYCOLLECTION(POINT(15 15),LINESTRING(16 16,3 15))\"))");
      statement.executeUpdate("INSERT INTO " + storageClassName + "(" + fieldName + ") VALUES (St_GeomFromText(\"POINT (12.4684635 41.8914114)\"))");
      statement.close();
      conn.close();
    } catch (SQLException exc) {
      exc.printStackTrace();
      return;
    }
    
    //execute bounding box filter
    try{
      String response = sendPost(geoServerUrl, bboxBody);
      //parse response and check if result is valid
      
      response = sendPost(geoServerUrl, intersectsBody);
      //parse response and check if result  is valid
      
      response = sendPost(geoServerUrl, notDisjointBody);
      //parse response and check if result  is valid
    }
    catch (Exception e){
      e.printStackTrace();
      return;
    }
  }

  private static String sendPost(String url, String postData) throws Exception {   
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(url);
    StringEntity se = new StringEntity(postData);
    post.setEntity(se);
    HttpResponse response = client.execute(post);    
    if (response.getStatusLine().getStatusCode() == 200){
      StringBuilder sb = new StringBuilder();
      HttpEntity responseEntity = response.getEntity();
      InputStream is = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      while (line != null){
        sb.append(line);
        line = reader.readLine();
      }
      return sb.toString();
    }
    return null;
  }

}
