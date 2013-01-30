package org.kun.solr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrCopyCrawler {

	public static void main(String[] args) throws SolrServerException, IOException, InterruptedException {
		Options options = getOptions();

		// create the parser
		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			
			int rowsChunk = 1000;
			int start = 0;
			int hardCommitNumber = -1;
			if (line.hasOption("r")) {
				rowsChunk = Integer.parseInt(line.getOptionValue("r"));
			}
			if (line.hasOption("i")) {
				start = Integer.parseInt(line.getOptionValue("i"));
			}
			if (line.hasOption("h")) {
				hardCommitNumber = Integer.parseInt(line.getOptionValue("h"));
			}
			String sourceUrl = line.getOptionValue("s");
			String targetUrl = line.getOptionValue("t");
			String query = line.getOptionValue("q");
			
			SolrCopyCrawler crawler = new SolrCopyCrawler(sourceUrl, targetUrl);
			crawler.solrCrawler(query, start, rowsChunk, hardCommitNumber);
			
		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SolrCopyCrawler", options);
		}

	}

	private static Options getOptions() {
		Options options = new Options();

		Option opt = new Option("s", "sourceUrl", true, "Solr source url");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("t", "targetUrl", true, "Solr target url");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("q", "query", true, "Query String");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("r", "rows", true, "number of rows chunk to process at a time");
		opt.setType(Number.class);
		opt.setRequired(false);
		options.addOption(opt);
		
		opt = new Option("h", "hardCommitNumber", true, "number records on which hard commit should be triggered");
		opt.setType(Number.class);
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("i", "start", true, "starting index");
		opt.setType(Number.class);
		opt.setRequired(false);
		options.addOption(opt);

		return options;
	}

	private final static int QUEUE_SIZE = 20;
	private final static int THREAD_COUNT = 2;
	
	private String sourceURL = "";
	private String targetURL = "";
	
	private SolrServer sourceServer;
	private SolrServer updateServer;
	private long totalFound;
	private int processedCount = 0;
	private int maxRetry = 20;
	private int retried = 0;
	private int sleepRetry = 6*60*1000;	//6 min
	private int hardCommittedCount = 0;

	public SolrCopyCrawler(SolrServer sourceServer, SolrServer updateServer){
		this.sourceServer = sourceServer;
		this.updateServer = updateServer;		
	}
	
	public SolrCopyCrawler(String sourceUrl, String targetUrl){
		this.sourceURL = sourceUrl;
		this.targetURL = targetUrl;
		sourceServer = new HttpSolrServer(sourceURL);
		updateServer = new ConcurrentUpdateSolrServer(targetURL, QUEUE_SIZE, THREAD_COUNT);		
		//updateServer = new HttpSolrServer(targetURL);
	}
	
	public void solrCrawler(String query, int start, int rowsChunk, int hardCommitNumber) throws InterruptedException, IOException, SolrServerException {
		processedCount = start;		
		SolrQuery solrQuery = new SolrQuery().setQuery(query).setRows(0);
		QueryResponse rsp = sourceServer.query(solrQuery);
		totalFound = rsp.getResults().getNumFound();
		log("Total Found = " + this.totalFound);
		boolean loopQuery = false;
		do {
			int currentStart = this.processedCount;
			try {
				loopQuery = doCopy(query, this.processedCount, rowsChunk);
			} catch (SolrServerException e) {
				e.printStackTrace();
				loopQuery = retry(currentStart);
			} catch (IOException e) {
				e.printStackTrace();
				loopQuery = retry(currentStart);
			}
			
			if((hardCommitNumber != -1) && (hardCommitNumber < hardCommittedCount)){
				try {
					this.updateServer.commit();
					hardCommittedCount = 0;
					writeRestartingPoint(currentStart);
				} catch (SolrServerException e) {
					e.printStackTrace();
					loopQuery = retry(currentStart);
				}
			}
			
		} while (loopQuery);
		updateServer.commit();
	}

	private boolean retry(int restartFrom) throws InterruptedException{
		if(this.retried > this.maxRetry){
			writeRestartingPoint(restartFrom);
			return false;
		}
		Thread.sleep(sleepRetry);
		this.retried++;
		updateServer = new ConcurrentUpdateSolrServer(this.targetURL, QUEUE_SIZE, THREAD_COUNT);
		this.hardCommittedCount = 0;
		this.processedCount = restartFrom;
		return true;
	}

	public String writeRestartingPoint(int restartFrom) {
		//Write restarting point
		try{
			  // Create file 
			  FileWriter fstream = new FileWriter("restartingPoint.properties");
			 
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write(restartFrom+"");
			  //Close the output stream
			  out.close();
			  File output = new File("restartingPoint.properties");
			  if(output.exists()){
				  return output.getAbsolutePath();
			  }
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		return null;
	}
	
	public boolean doCopy(String query, int start, int rows) throws SolrServerException, IOException {
		log("Start = " + (start + 1));
		SolrQuery solrQuery = new SolrQuery().setQuery(query).setStart(start).setRows(rows);
		QueryResponse rsp = sourceServer.query(solrQuery);
		SolrDocumentList list = rsp.getResults();
		if (list.size() == 0){
			return false;
		}
		for (SolrDocument doc : list) {
			SolrInputDocument copy = toSolrInputDocument(doc);
			this.updateServer.add(copy);
			this.processedCount++;
			this.hardCommittedCount++;
			log(doc.get("id").toString() + " processed: " + (this.processedCount));
		}
		return true;
	}

	public static SolrInputDocument toSolrInputDocument(SolrDocument d) {
		SolrInputDocument doc = new SolrInputDocument();
		for (String name : d.getFieldNames()) {
			if (name.endsWith("_dt") && (d.getFieldValue(name) instanceof ArrayList)) {
				ArrayList values = (ArrayList) d.getFieldValue(name);
				doc.addField(name, values.get(values.size() - 1), 1.0f);
			} else if(name.equals("_version_")){
				doc.addField(name, 0);
			} else {
				doc.addField(name, d.getFieldValue(name), 1.0f);
			}
		}
		return doc;
	}

	public static void log(String message) {
		System.out.println(message);
	}
	
	public int getMaxRetry() {
		return maxRetry;
	}

	public void setMaxRetry(int maxRetry) {
		this.maxRetry = maxRetry;
	}

	public int getRetried() {
		return retried;
	}

	public void setRetried(int retried) {
		this.retried = retried;
	}

	public String getSourceURL() {
		return sourceURL;
	}

	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}

	public String getTargetURL() {
		return targetURL;
	}

	public void setTargetURL(String targetURL) {
		this.targetURL = targetURL;
	}

	public int getSleepRetry() {
		return sleepRetry;
	}

	public void setSleepRetry(int sleepRetry) {
		this.sleepRetry = sleepRetry;
	}

	public SolrServer getSourceServer() {
		return sourceServer;
	}

	public void setSourceServer(SolrServer sourceServer) {
		this.sourceServer = sourceServer;
	}

	public SolrServer getUpdateServer() {
		return updateServer;
	}

	public void setUpdateServer(SolrServer updateServer) {
		this.updateServer = updateServer;
	}
	

}
