package org.kun.solr;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.SolrServerException;
import org.kun.solr.benchmark.SolrCloudBenchmark;

public class SolrBenchmark {

	/**
	 * @param args
	 * @throws SolrServerException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, SolrServerException {
		Options options = getOptions();

		// create the parser
		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			String targetUrl = "";
			String queryFile = "";
			String fieldFile = null;
			String fqFile = null;
			int numberOfQuery = 0;
			if (line.hasOption("t")) {
				targetUrl = line.getOptionValue("t");
			}
			if (line.hasOption("q")) {
				queryFile = line.getOptionValue("q");
			}
			if (line.hasOption("f")) {
				fieldFile = line.getOptionValue("f");
			}
			if (line.hasOption("fq")) {
				fqFile = line.getOptionValue("fq");
			}
			if (line.hasOption("n")) {
				numberOfQuery = Integer.parseInt(line.getOptionValue("n"));
			}
			SolrCloudBenchmark benchmark = new SolrCloudBenchmark(targetUrl, queryFile);
			benchmark.setFieldFile(fieldFile);
			benchmark.setFqFile(fqFile);
			if(numberOfQuery > 0){
				benchmark.setNumberOfQuery(numberOfQuery);
			}
			double result = benchmark.start();
			System.out.println("=====================================");
			System.out.println("Average QTime : " + result);
		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("SolrBenchmark", options);
		}
	}
	

	private static Options getOptions() {
		Options options = new Options();

		Option opt  = new Option("t", "targetUrl", true, "Solr target url");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("q", "queryFile", true, "Query file list");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("f", "facetFieldFile", true, "fields to faceting test");
		opt.setRequired(false);
		options.addOption(opt);
		
		opt = new Option("fq", "filterQueryFile", true, "filter query file list");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("n", "numberOfQuery", true, "Number of queries");
		opt.setType(Number.class);
		opt.setRequired(false);
		options.addOption(opt);

		return options;
	}

}
