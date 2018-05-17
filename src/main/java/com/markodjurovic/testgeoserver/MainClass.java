/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.markodjurovic.testgeoserver;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Idea is to insert some data into orientDB and to execute some queries via
 * geoserver WFS. Features testing is taken from geoserver demo
 *
 * @author mdjurovi
 */
public class MainClass {

//  private final static String USER_AGENT = "TestApp";
  
  private static final Set<String> geometryNodesNames = new HashSet<>(Arrays.asList("gml:Point", "gml:LineString"));
  
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
"        <PropertyName>location</PropertyName>\n" +
"          <gml:Point srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\">\n" +
"            <gml:coordinates>20,20</gml:coordinates>\n" +
"          </gml:Point>\n" +
"        </Intersects>\n" +
"      </Filter>\n" +
"  </wfs:Query>\n" +
"</wfs:GetFeature>";
  
  //this one should return oposite result than intersects
  private static final String disjointBody = "<wfs:GetFeature service=\"WFS\" version=\"1.0.0\"\n" +
"  outputFormat=\"GML2\"\n" +
"  xmlns:topp=\"http://www.openplans.org/topp\"\n" +
"  xmlns:wfs=\"http://www.opengis.net/wfs\"\n" +
"  xmlns:ogc=\"http://www.opengis.net/ogc\"\n" +
"  xmlns:gml=\"http://www.opengis.net/gml\"\n" +
"  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
"  xsi:schemaLocation=\"http://www.opengis.net/wfs\n" +
"                      http://schemas.opengis.net/wfs/1.0.0/WFS-basic.xsd\">\n" +
"   <wfs:Query typeName=\"test:Restaurant\">\n" +
"      <ogc:Filter>\n" +
"   		<ogc:Disjoint>\n" +
"      		<ogc:PropertyName>location</ogc:PropertyName>\n" +
"      		<gml:Point srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\">\n" +
"        		<gml:coordinates>20,20</gml:coordinates>\n" +
"      		</gml:Point>\n" +
"   		</ogc:Disjoint>\n" +
"	  </ogc:Filter>\n" +
"   </wfs:Query>\n" +
"</wfs:GetFeature>";

  public static void main(String[] args) {
    String storageClassName = "Restaurant";
    String fieldName = "location";
    String database = args[0];
    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");
    String geoServerUrl = "http://localhost:8080/geoserver";
    geoServerUrl = "http://localhost:8080/geoserver/wfs";

//    OrientDB orient = new OrientDB("remote:localhost", "root", "000000", OrientDBConfig.defaultConfig());
//    orient.createIfNotExists(database, ODatabaseType.PLOCAL);
//    ODatabaseSession db = orient.open(database, "admin", "admin");
//    OClass storageClass = db.createClassIfNotExist(storageClassName);    
//    if (storageClass.getProperty(fieldName) != null)
//      storageClass.dropProperty(fieldName);
    
    try {
            
      Connection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:remote:localhost/" + database, info);
      Statement statement = conn.createStatement();
//      statement.execute("CREATE PROPERTY " + storageClassName + "." + fieldName + " EMBEDDED OGeometry");
//      statement.execute("CREATE INDEX " + fieldName + ".index ON " + storageClassName + "(" + fieldName + ") SPATIAL ENGINE LUCENE");
      statement.executeUpdate("INSERT INTO " + storageClassName + "(" + fieldName + ") VALUES (St_GeomFromText(\"LINESTRING (30 10, 10 30, 40 40)\"))");
      statement.executeUpdate("INSERT INTO " + storageClassName + "(" + fieldName + ") VALUES (St_GeomFromText(\"GEOMETRYCOLLECTION(POINT(15 15),LINESTRING(16 16,3 15))\"))");
      statement.executeUpdate("INSERT INTO " + storageClassName + "(" + fieldName + ") VALUES (St_GeomFromText(\"POINT (12 41)\"))");
      statement.close();
      conn.close();
    } catch (SQLException exc) {
      exc.printStackTrace();
      return;
    }
        
    try{
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();      
      
      String response = sendPost(geoServerUrl, bboxBody);
      InputSource is = new InputSource(new StringReader(response));
      Document doc = dBuilder.parse(is);
      Node rootNode = doc.getChildNodes().item(0);
      List<Node> nodes = getGeometryNodes(rootNode);
      List<TypeCoordinates> typeCoordinates = getTypeAndCoordinatesFromGeomNodes(nodes);
      int numOfLines = getNoOfType(typeCoordinates, Type.LINE);
      if (numOfLines != 2){
        System.err.println("Invalid num of lines for bounding box check");
      }
      int numOfPoints = getNoOfType(typeCoordinates, Type.POINT);
      if (numOfPoints != 2){
        System.err.println("Invalid num of points for bounding box check");
      }
      
      response = sendPost(geoServerUrl, intersectsBody);
      doc = dBuilder.parse(new InputSource(new StringReader(response)));
      rootNode = doc.getChildNodes().item(0);
      nodes = getGeometryNodes(rootNode);
      typeCoordinates = getTypeAndCoordinatesFromGeomNodes(nodes);
      numOfLines = getNoOfType(typeCoordinates, Type.LINE);
      if (numOfLines != 1){
        System.err.println("Invalid num of lines for intersects check");
      }
      numOfPoints = getNoOfType(typeCoordinates, Type.POINT);
      if (numOfPoints != 0){
        System.err.println("Invalid num of points for intersects check");
      }
      
      response = sendPost(geoServerUrl, disjointBody);
      doc = dBuilder.parse(new InputSource(new StringReader(response)));
      rootNode = doc.getChildNodes().item(0);
      nodes = getGeometryNodes(rootNode);
      typeCoordinates = getTypeAndCoordinatesFromGeomNodes(nodes);
      numOfLines = getNoOfType(typeCoordinates, Type.LINE);
      if (numOfLines != 1){
        System.err.println("Invalid num of lines for disjoint check");
      }
      numOfPoints = getNoOfType(typeCoordinates, Type.POINT);
      if (numOfPoints != 2){
        System.err.println("Invalid num of points for disjoint check");
      }
    }
    catch (Exception e){
      e.printStackTrace();
      return;
    }
    
    System.out.println("Everything is OK");
    
//    try{
//      Connection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:remote:localhost/" + database, info);
//      Statement statement = conn.createStatement();
//      statement.executeUpdate("DELETE FROM " + storageClassName);
//      
//      statement.close();
//      conn.close();
//    }
//    catch (SQLException exc){
//      exc.printStackTrace();
//    }
    
//    orient.drop(database);
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
  
  //valid only for test
  private static List<Node> getGeometryNodes(Node rootNode){
    List<Node> retList = new ArrayList<>();
    NodeList childNodes = rootNode.getChildNodes();
    if (childNodes == null){
      return retList;
    }
    for (int i = 0; i < childNodes.getLength(); i++){
      Node childNode = childNodes.item(i);
      if (isGeomNode(childNode)){
        retList.add(childNode);
      }
      else{
        retList.addAll(getGeometryNodes(childNode));
      }
    }
    return retList;
  }

  private static boolean isGeomNode(Node node) {
    String nodeName = node.getNodeName();
    return geometryNodesNames.contains(nodeName);
  }

  private static void fillCoordinates(TypeCoordinates retVal, Node valueNode) {
    if (valueNode.getNodeType() == Node.ELEMENT_NODE){      
      String coords = valueNode.getTextContent();
      String[] tokens = coords.split(",| ");
      for (String token : tokens){
        double val = Double.parseDouble(token);
        retVal.addCoordinate(val);
      }
    }
    return;
  }
  
  enum Type{
    UNDEFINIED,
    POINT,
    LINE
  }
  
  private static class TypeCoordinates{
    Type type;
    List<Double> coordinates = new ArrayList<>();
    
    void addCoordinate(double coord){
      coordinates.add(coord);
    }
  }
  
  static List<TypeCoordinates> getTypeAndCoordinatesFromGeomNodes(List<Node> geomNodes){
    List<TypeCoordinates> retVal = new ArrayList<>();
    for (Node node : geomNodes){
      TypeCoordinates tco = getTypeAndCoordinatesFromGeomNode(node);
      retVal.add(tco);
    }
    
    return retVal;
  }
  
  static TypeCoordinates getTypeAndCoordinatesFromGeomNode(Node geomNode){
    TypeCoordinates retVal = new TypeCoordinates();
    
    String name = geomNode.getNodeName();
    Type type = getTypeFromName(name);
    
    retVal.type = type;
    
    NodeList children = geomNode.getChildNodes();
    for (int i = 0; i < children.getLength(); i++){
      Node child = children.item(i);
      if (child.getNodeName().equals("gml:coordinates")){
        fillCoordinates(retVal, child);
        return retVal;
      }
    }
    
    return null;
  }

  static Type getTypeFromName(String name){
    switch (name){
      case "gml:LineString":
        return Type.LINE;
      case "gml:Point":
        return Type.POINT;
      default:
        return Type.UNDEFINIED;
    }
  }
  
  static int getNoOfType(List<TypeCoordinates> typesCoordinates, Type wantedType){
    int counter = 0;
    counter = typesCoordinates.stream().mapToInt((TypeCoordinates value) -> {
      if (value.type == wantedType)
        return 1;
      else
        return 0;      
    }).reduce(0, (int left, int right) -> left + right);
    return counter;
  }
  
}
