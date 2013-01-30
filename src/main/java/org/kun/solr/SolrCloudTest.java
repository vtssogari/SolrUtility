package org.kun.solr;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrCloudTest {

	
	public static void delete() throws SolrServerException, IOException{
		SolrServer server = new HttpSolrServer("http://192.168.1.125:8080/solr/i2k_shard2_replica1");
		UpdateResponse rsp = server.deleteByQuery("r_creation_date:[NOW-4YEAR/DAY TO NOW]");
		server.commit();
		System.out.println(rsp.toString());
		
	}
	
	public static void test1(){

		CloudSolrServer server = null;
		try {
			server = new CloudSolrServer("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		server.setDefaultCollection("udid_udi");
		
		SolrInputDocument doc1 = new SolrInputDocument();
	    doc1.addField( "id", "abc", 1.0f );
	    doc1.addField( "name_txt", "doc1", 1.0f );
	    doc1.addField( "price_txt", "234" );
	    
	    try {
			server.add( doc1 );
		} catch (SolrServerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    try {
			server.commit();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
