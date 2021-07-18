package org.aksw.deer.plugin.old;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.aksw.deer.enrichments.AbstractParameterizedEnrichmentOperator;
import org.aksw.deer.plugin.example.OntologyMatchingOperator;
import org.aksw.deer.vocabulary.DEER;
import org.aksw.faraday_cage.engine.ValidatableParameterMap;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.resultset.ResultSetMem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OntologyMatchingOperator_Test extends AbstractParameterizedEnrichmentOperator {

	private static final Logger logger = LoggerFactory.getLogger(OntologyMatchingOperator.class);
	public static final Property SUBJECT = DEER.property("subject");
	public static final Property PREDICATE = DEER.property("predicate");
	public static final Property OBJECT = DEER.property("object");
	public static final Property SELECTOR = DEER.property("selector");
	public static final Property SPARQL_CONSTRUCT_QUERY = DEER.property("sparqlConstructQuery");

	public OntologyMatchingOperator_Test() {
		super();
	}

	@Override
	public ValidatableParameterMap createParameterMap() {

		return ValidatableParameterMap.builder().declareProperty(SELECTOR).declareProperty(SPARQL_CONSTRUCT_QUERY)
				.declareValidationShape(getValidationModelFor(OntologyMatchingOperator.class)).build();
	}

	@Override
	protected List<Model> safeApply(List<Model> data) {

		System.out.println("Inside Ontology Matcher Model");
		ontologyMatchImpl();
		
		return null;
	}
	
	public void ontologyMatchImpl() {

		/*
		 * DefaultClassMapper defauOnt = new DefaultClassMapper(); defauOnt.
		 * 
		 * LabelBasedClassMapper label = new LabelBasedClassMapper(); AMapping
		 * entityMapping =
		 * label.getEntityMapping("https://yago-knowledge.org/sparql/query",
		 * "http://dbpedia.org/sparql"); entityMapping.getMap();
		 */

		String query = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
				+ "PREFIX dbpedia0: <http://dbpedia.org/ontology/>" + "PREFIX dbpedia2: <http://dbpedia.org/property/>"
				+ "SELECT DISTINCT ?s ?p ?o\r\n"
				+ "WHERE { \r\n"
				+ "				 ?s rdfs:subClassOf  dbpedia0:Settlement.\r\n"
				+ "   ?s ?p ?o.\r\n"
				+ "}ORDER BY ASC(?s) LIMIT 100"; // Query

		String endpointStr = "http://dbpedia.org/sparql"; // sparql end-point

		ResultSet execSelect = QueryExecutionFactory.sparqlService(endpointStr, query).execSelect();

		ResultSetMem result = new ResultSetMem(execSelect);
		List<QuerySolution> qsListSPARQL = new ArrayList<>();

		while (result.hasNext()) {
			QuerySolution next = result.next();
			qsListSPARQL.add(next);
			 System.out.println(next);
			//break;
		}
		System.out.println("--Execution completed for DB PEDIA SPARQL ENDPOINT--");


		
		// Fetching details from yago
		//PREFIXS
		/*https://en.wikibooks.org/wiki/SPARQL/Prefixes
		 * PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wds:
		 * <http://www.wikidata.org/entity/statement/> PREFIX wdv:
		 * <http://www.wikidata.org/value/> PREFIX wdt:
		 * <http://www.wikidata.org/prop/direct/> PREFIX wikibase:
		 * <http://wikiba.se/ontology#> PREFIX p: <http://www.wikidata.org/prop/> PREFIX
		 * ps: <http://www.wikidata.org/prop/statement/> PREFIX pq:
		 * <http://www.wikidata.org/prop/qualifier/> PREFIX rdfs:
		 * <http://www.w3.org/2000/01/rdf-schema#> PREFIX bd:
		 * <http://www.bigdata.com/rdf#>
		 */		
		
		query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
		 		+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
		 		+ "PREFIX schema: <http://schema.org/>"
		 		+ "SELECT  DISTINCT ?s ?p ?o\r\n"
		 		+ "WHERE \r\n"
		 		+ "{\r\n"
		 		+ "	 ?s rdfs:subClassOf* schema:AdministrativeArea   .\r\n"
		 		+ "    ?s ?p ?o.\r\n"
		 		+ "   \r\n"
		 		+ "} \r\n"
		 		+ "ORDER BY ASC (?s)";

		endpointStr = "https://yago-knowledge.org/sparql/query";
		execSelect = QueryExecutionFactory.sparqlService(endpointStr, query).execSelect();
		result = new ResultSetMem(execSelect);
		List<QuerySolution> qsListYAGO = new ArrayList<>();
		while (result.hasNext()) {
			QuerySolution next = result.next();
			qsListYAGO.add(next);
			System.out.println(next);
			
		}
		System.out.println("--Execution completed for YAGO SPARQL ENDPOINT--");
		
		// File initialization for SPARQL
		File sparqlFile = new File("sparqldata.ttl");
		FileOutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(sparqlFile);
		} catch (FileNotFoundException e) {
			System.out.println("Exception while creating a file.");
			e.printStackTrace();
		}
		
		//Iterating Query Solutions
		System.out.println("DB pedia data : ");
		for(QuerySolution querySol : qsListSPARQL) { 
			//Iterator<String> varNames = querySol.varNames();
			List<String> triple = new ArrayList<>();
			//while(varNames.hasNext()) {
				//String key = varNames.next();
				//RDFNode rdfNode = querySol.get(key);
				//System.out.println(rdfNode.toString());
				//triple.add(rdfNode.toString());
			//}
			
			String tripleData = "<" + querySol.get("s") +">	 <" + querySol.get("p") + ">	 <" + querySol.get("o") + ">\n";
			try {
				//Write triple data in file
				fileOut.write(tripleData.getBytes());
			} catch (IOException e) {
					System.out.println("Issue while writing in file");
				e.printStackTrace();
			}
			
			
		}
		
		
		try {
			fileOut.close();
		} catch (IOException e) {
			System.out.println("Issue while closing the file");
			e.printStackTrace();
		}
		
		
		// File creation logic for YAGO
		File yagoFile = new File("yagodata.ttl");
		try {
			fileOut = new FileOutputStream(yagoFile);
		} catch (FileNotFoundException e) {
			System.out.println("Exception while creating a file.");
			e.printStackTrace();
		}
		
		System.out.println("YAGO data : ");
		for(QuerySolution querySol : qsListYAGO) {
			Iterator<String> varNames = querySol.varNames();
			List<String> triple = new ArrayList<>();
			while(varNames.hasNext()) {
				String key = varNames.next();
				RDFNode rdfNode = querySol.get(key);
				//System.out.println(rdfNode.toString());
				triple.add(rdfNode.toString());
			}
			
			String tripleData = "<" + triple.get(0) +">	 <" + triple.get(1) + ">	 <" + triple.get(2) + ">\n";
			try {
				//Write triple data in file
				fileOut.write(tripleData.getBytes());
			} catch (IOException e) {
					System.out.println("Issue while writing in file");
				e.printStackTrace();
			}
		}
		
		try {
			fileOut.close();
		} catch (IOException e) {
			System.out.println("Issue while closing the file");
			e.printStackTrace();
		}
		
		
		//Read files and match
		Scanner sc = null;
		List<String> sparqlTripleData = new ArrayList<>();
		try {
			sc = new Scanner(new File("sparqldata.ttl"));
			while (sc.hasNextLine()) {
				sparqlTripleData.add(sc.nextLine());
				/*
				 * sparqlData = sc.nextLine().substring(sparqlData.lastIndexOf('/'),
				 * sparqlData.lastIndexOf('>')); System.out.println("SPARQL DATA : ");
				 */
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		List<String> yagoTripleData = new ArrayList<>();
		try {
			sc = new Scanner(new File("yagodata.ttl"));
			while (sc.hasNextLine()) {
				yagoTripleData.add(sc.nextLine());
				/*
				 * sparqlData = sc.nextLine().substring(sparqlData.lastIndexOf('/'),
				 * sparqlData.lastIndexOf('>')); System.out.println("SPARQL DATA : ");
				 */
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(String tripleSPARQL : sparqlTripleData) {
			String keywordSPARQL = tripleSPARQL.substring(0, tripleSPARQL.indexOf(' '));
			keywordSPARQL = keywordSPARQL.substring(keywordSPARQL.lastIndexOf('/') + 1, keywordSPARQL.lastIndexOf('>'));
			System.out.println("Matching pattern in SPARQL : "  + keywordSPARQL);
			int counter = 0;
			for(String tripleYAGOO : yagoTripleData) {
				if(tripleYAGOO.contains(keywordSPARQL)){
					//System.out.println("SPARQL : " + tripleSPARQL);
					//System.out.println("YAGOO : " + tripleYAGOO);
					counter++;
				}
			}
			System.out.println("Keyword : " + keywordSPARQL + ",Count : " + counter);
		}
		
	}
}
