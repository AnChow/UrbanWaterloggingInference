package zyl.ConvertToJsonld;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlSchema;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.ModelFactory;

import com.github.jsonldjava.core.RDFDataset.Literal;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ConvertData {
	MongoClient mongoclient;
	DB db;
	DBCollection coll;
	List<String> cityList = Arrays.asList("上海","北京","南京","天津","广州","杭州","武汉","沈阳","石家庄","重庆");
	//List<String> cityList = Arrays.asList("上海");
	public void convertAll(){
		mongoclient = new MongoClient();
		db = mongoclient.getDB("Air");
		
		for(int i=0;i<cityList.size();i++){
			convertCity(cityList.get(i));
		}
		
		mongoclient.close();
	}
	
	private void convertCity(String city){
		System.out.println(city);
		OntModel m;
		OntClass smog;
		
		m = ModelFactory.createOntologyModel();
		smog = m.getOntClass(Prefix.schema+"smog");
		m.setNsPrefix("nsa",Prefix.schema);
		m.removeNsPrefix("rdf");
		m.removeNsPrefix("owl");
		m.removeNsPrefix("xsd");
		m.removeNsPrefix("rdfs");
		
		BasicDBObject query = new BasicDBObject();
		query.append("area", city);
		query.append("time_point", new BasicDBObject("$gt","2014-02-01T00:00:00Z"));
		DBCursor cursor = db.getCollection("Stations").find(query);
		
		while(cursor.hasNext()){
			DBObject o = cursor.next();
			String positionName = (String)o.get("position_name");
			if(positionName==null)
				positionName = (String)o.get("station_name");
			String timePoint = (String)o.get("time_point");
			Individual poi = m.createIndividual(Prefix.individual+positionName+"-"+timePoint,smog);
			m.add(poi,m.getProperty(Prefix.schema,"hasSensorLocation"),m.createTypedLiteral(positionName,Prefix.xsd+"String"));
			
			m.add(poi,m.getProperty(Prefix.schema,"hasTimePoint"),m.createTypedLiteral(timePoint,Prefix.xsd+"String"));
			m.add(poi,m.getProperty(Prefix.schema,"hasAQI"),m.createTypedLiteral((int)getValue(o,"aqi")));
			m.add(poi,m.getProperty(Prefix.schema,"hasQuality"),m.createTypedLiteral((String)getValue(o,"quality"),Prefix.xsd+"String"));
			m.add(poi,m.getProperty(Prefix.schema,"hasPm25"),m.createTypedLiteral((int)getValue(o,"pm2_5")));
			m.add(poi,m.getProperty(Prefix.schema,"hasPm10"),m.createTypedLiteral((int)getValue(o,"pm10")));
			m.add(poi,m.getProperty(Prefix.schema,"hasPrimaryPollution"),m.createTypedLiteral((String)getValue(o, "primary_pollutant"),Prefix.xsd+"String"));
		}
		cursor.close();
		
		try {
			m.write(new FileOutputStream("./smog-"+city+".jsonld"),"JSON-LD");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end");
	}
	private Object getValue(DBObject o, String key){
		Object tmp = o.get(key);
		if(tmp==null)
			return "";
		else
			return tmp;
	}
}
