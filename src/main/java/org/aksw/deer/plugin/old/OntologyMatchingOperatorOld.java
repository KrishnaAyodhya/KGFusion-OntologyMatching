package org.aksw.deer.plugin.old;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

import org.aksw.deer.enrichments.AbstractParameterizedEnrichmentOperator;
import org.aksw.deer.vocabulary.DEER;
import org.aksw.faraday_cage.engine.ValidatableParameterMap;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.writer.RDFConfigurationWriter;
import org.aksw.limes.core.ml.algorithm.LearningParameter;
import org.aksw.limes.core.ml.algorithm.MLImplementationType;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.SimpleSelector;
import org.apache.jena.sparql.resultset.ResultSetMem;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Extension
public class OntologyMatchingOperatorOld extends AbstractParameterizedEnrichmentOperator {

	private static final Logger logger = LoggerFactory.getLogger(OntologyMatchingOperatorOld.class);

	public static final Property SUBJECT = DEER.property("subject");
	public static final Property PREDICATE = DEER.property("predicate");
	public static final Property OBJECT = DEER.property("object");
	public static final Property SELECTOR = DEER.property("selector");
	public static final Property SPARQL_CONSTRUCT_QUERY = DEER.property("sparqlConstructQuery");

	public OntologyMatchingOperatorOld() {

		super();
	}

	@Override
	public ValidatableParameterMap createParameterMap() { // 2
		return ValidatableParameterMap.builder().declareProperty(SELECTOR).declareProperty(SPARQL_CONSTRUCT_QUERY)
				.declareValidationShape(getValidationModelFor(OntologyMatchingOperatorOld.class)).build();
	}

	@Override
	protected List<Model> safeApply(List<Model> models) { // 3
		Model a = filterModel(models.get(0));
	 
		
		System.out.println("Krishna's Model ");
		
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

			List<String> triple = new ArrayList<>();
			
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

			}
		} catch (FileNotFoundException e) {
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
					System.out.println("SPARQL : " + tripleSPARQL);
					System.out.println("YAGOO : " + tripleYAGOO);
					counter++;
				}
			}
			System.out.println("Keyword : " + keywordSPARQL + ",Count : " + counter);
		}
		
		
		
		//callLimes(con);

	 	// create an empty Model
		Model model = ModelFactory.createDefaultModel();
		return List.of(model);
	}

	private Model filterModel(Model model) { // 4

		final Model resultModel = ModelFactory.createDefaultModel();
		final Optional<RDFNode> sparqlQuery = getParameterMap().getOptional(SPARQL_CONSTRUCT_QUERY);
		if (sparqlQuery.isPresent()) {
			logger.info("Executing SPARQL CONSTRUCT query for " + getId() + " ...");
			return QueryExecutionFactory.create(sparqlQuery.get().asLiteral().getString(), model).execConstruct();
		} else {
			getParameterMap().listPropertyObjects(SELECTOR).map(RDFNode::asResource).forEach(selectorResource -> {
				RDFNode s = selectorResource.getPropertyResourceValue(SUBJECT);
				RDFNode p = selectorResource.getPropertyResourceValue(PREDICATE);
				Resource o = selectorResource.getPropertyResourceValue(OBJECT);

				logger.info("Running filter " + getId() + " for triple pattern {} {} {} ...",
						s == null ? "[]" : "<" + s.asResource().getURI() + ">",
						p == null ? "[]" : "<" + p.asResource().getURI() + ">",
						o == null ? "[]" : "(<)(\")" + o.toString() + "(\")(>)");
				SimpleSelector selector = new SimpleSelector(s == null ? null : s.asResource(),
						p == null ? null : p.as(Property.class), o);
				resultModel.add(model.listStatements(selector));
			});
		}
		return resultModel;
	}



}
