package org.kun.solr.benchmark;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

public class SolrCloudBenchmark {

	private String queryFile;
	private String fieldFile;
	private String fqFile;
	private  int numberOfQuery = 100;
	
	private SolrServer server;
	
	public SolrCloudBenchmark(String targetUrl, String queryFile) throws MalformedURLException{
		this.queryFile = queryFile;
		if(targetUrl.contains(",")){
			server = new LBHttpSolrServer(targetUrl.split(","));
		}else{
			server = new LBHttpSolrServer(targetUrl);
		}
	}

	public double start() throws IOException, SolrServerException{
		int queryIndex = 0;
		int fieldIndex = 0;
		int filterQueryIndex = 0;
		Calculation cal = new Calculation();
		List<String> queryList = readFileLines(this.queryFile);
		List<String> fieldsList = readFileLines(this.fieldFile);
		List<String> filterQueryList = readFileLines(this.fqFile);
		if(queryList.size() > 0){
			for(int i=0; i < numberOfQuery; i++){
				queryIndex = getNextIndex(queryList);
				fieldIndex = getNextIndex(fieldsList);
				filterQueryIndex = getNextIndex(filterQueryList);
				
				//String query = "\"" + queryList.get(queryIndex) + " " + queryList.get(queryIndex2) + "\"~5";
				String query =queryList.get(queryIndex);
				String facet = fieldIndex == -1 ? null : fieldsList.get(fieldIndex);
				String fq = filterQueryIndex == -1 ? null : filterQueryList.get(filterQueryIndex);
				QueryResponse rsp = query(query, facet, fq);
				cal.addQTime(rsp.getQTime());
				log(i + " qtime : " + rsp.getQTime() + "\t\t Average : " + cal.getAverageTime() + "\tq=" + query + "\tfacet=" + facet + "\tfound=" + rsp.getResults().getNumFound());
			}
		}
		return cal.getAverageTime();
	}
	
	private int getNextIndex(List<String> list){
		if(list.size() == 0){
			return -1;
		}
		Random rand = new Random();
		int min = 0;
		int max = list.size() - 1;

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt(max - min + 1) + min;
		return randomNum;
		
	}
	
	private QueryResponse query(String q, String facetField, String filterQuery) throws SolrServerException{
	    SolrQuery query = new SolrQuery();
	    query.setQuery(q);
	    if(facetField != null){
	    	query.addFacetField(facetField);
	    }
	    if(filterQuery != null){
	    	query.addFilterQuery(filterQuery);
	    }
	    QueryResponse rsp = server.query(query);
	    return rsp;
	}
	
	private List<String> readFileLines(String filePath) throws IOException{
		List<String> result = new ArrayList<String>();
		if(filePath == null){
			return result;
		}
		File f = new File(filePath);
		if(f.exists()){
			FileInputStream fstream = new FileInputStream(filePath);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				result.add(strLine);
			}
			in.close();
		}
		return result;
	}
	
	private void log(String msg){
		System.out.println(msg);
	}
	
	public String getFieldFile() {
		return fieldFile;
	}

	public void setFieldFile(String fieldFile) {
		this.fieldFile = fieldFile;
	}

	public String getFqFile() {
		return fqFile;
	}

	public void setFqFile(String fqFile) {
		this.fqFile = fqFile;
	}

	public int getNumberOfQuery() {
		return numberOfQuery;
	}

	public void setNumberOfQuery(int numberOfQuery) {
		this.numberOfQuery = numberOfQuery;
	}
	

}
