package org.aksw.deer.plugin.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.aksw.deer.enrichments.AbstractParameterizedEnrichmentOperator;
import org.aksw.deer.vocabulary.DEER;
import org.aksw.faraday_cage.engine.ValidatableParameterMap;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.SimpleSelector;
import org.pf4j.Extension;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.krr.logmap2.LogMap2_Matcher;
import uk.ac.ox.krr.logmap2.mappings.objects.MappingObjectStr;

/**
 */
@Extension
public class OntologyMatchingOperator extends AbstractParameterizedEnrichmentOperator {
 
	private static final Logger logger = LoggerFactory.getLogger(OntologyMatchingOperator.class);

	public static final Property SUBJECT = DEER.property("subject");
	public static final Property PREDICATE = DEER.property("predicate");
	public static final Property OBJECT = DEER.property("object");
	public static final Property SELECTOR = DEER.property("selector");
	public static final Property SPARQL_CONSTRUCT_QUERY = DEER.property("sparqlConstructQuery");

	public OntologyMatchingOperator() {

		super();
	}

	@Override
	public ValidatableParameterMap createParameterMap() { // 2
		return ValidatableParameterMap.builder().declareProperty(SELECTOR).declareProperty(SPARQL_CONSTRUCT_QUERY)
				.declareValidationShape(getValidationModelFor(OntologyMatchingOperator.class)).build();
	}

	@Override
	protected List<Model> safeApply(List<Model> models) { // 3
		Model a = filterModel(models.get(0));

		System.out.println("Krishna's Model  ");
		System.out.println("Krishna");
		// sparql end-point

		String endpointStr = "http://dbpedia.org/sparql";

		String queryYagoo = "CONSTRUCT {?s ?p ?o} WHERE {  ?s ?p ?o.\r\n"
				+ "			   ?s  <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://schema.org/Movie>   .\r\n"
				+ "							 FILTER(!isBlank(?s) && !isLiteral(?o) && !isBlank(?o))\r\n"
				+ "						}LIMIT 1000";

		String queryDbpedia = "CONSTRUCT {?s ?p ?o} WHERE {  ?s ?p ?o.\r\n"
				+ "				 ?s <http://www.w3.org/2000/01/rdf-schema#subClassOf>  <http://dbpedia.org/ontology/Work>.\r\n"
				+ "								  FILTER(! isBlank(?s) && !isLiteral(?o))\r\n"
				+ "							}LIMIT 1000";

		// Dbpedia model
		Model modelDbpedia = QueryExecutionFactory.sparqlService(endpointStr, queryDbpedia).execConstruct();
		try {
			modelDbpedia.write(new FileOutputStream("dbpedia2.ttl"), "TTL");
		} catch (FileNotFoundException e) {
			System.out.println("Issue while creating DBPedia File");
			e.printStackTrace();
		}

		// Yago model
		endpointStr = "https://yago-knowledge.org/sparql/query";
		Model modelYago = QueryExecutionFactory.sparqlService(endpointStr, queryYagoo).execConstruct();
		try {
			modelYago.write(new FileOutputStream("yagoo2.ttl"), "TTL");
		} catch (FileNotFoundException e) {
			System.out.println("Issue while creating Yago File");
			e.printStackTrace();
		}

		// evaluationFun();

		OWLOntology onto1 = null;
		OWLOntology onto2 = null;

		OWLOntologyManager onto_manager;

		String onto1_iri = "yagoo2.ttl";
		String onto2_iri = "dbpedia2.ttl";

		onto_manager = OWLManager.createOWLOntologyManager();

		try {
			onto1 = onto_manager.loadOntologyFromOntologyDocument(new File(onto1_iri));
		} catch (OWLOntologyCreationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			onto2 = onto_manager.loadOntologyFromOntologyDocument(new File(onto2_iri));
		} catch (OWLOntologyCreationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// onto1 = onto_manager.loadOntology(IRI.create(onto1_iri));
		// onto2 = onto_manager.loadOntology(IRI.create(onto2_iri));

		LogMap2_Matcher logmap2 = new LogMap2_Matcher(onto1, onto2);
		/*
		 * System.out.println("IRIs:"); System.out.println(logmap2.getIRIOntology1());
		 * System.out.println(logmap2.getIRIOntology2());
		 */

		/*
		 * // anchors System.out.println("Anchors : "); Set<MappingObjectStr>
		 * logmap2_anchors = logmap2.getLogmap2_anchors(); for(MappingObjectStr element:
		 * logmap2_anchors) { System.out.println("URL 1 : " + element.getIRIStrEnt1());
		 * System.out.println("URL 2 : " + element.getIRIStrEnt2()); }
		 */

		Set<String> representativeLabelsForMappings = logmap2.getRepresentativeLabelsForMappings();
		System.out.println("Labels : " + representativeLabelsForMappings);

		// Optionally LogMap also accepts the IRI strings as input
		// LogMap2_Matcher logmap2 = new LogMap2_Matcher(onto1_iri, onto2_iri);

		// Set of mappings computed my LogMap
		Set<MappingObjectStr> logmap2_mappings = logmap2.getLogmap2_Mappings();

		Iterator<MappingObjectStr> iterator = logmap2_mappings.iterator();

		Model model = ModelFactory.createDefaultModel();

		while (iterator.hasNext()) {
			MappingObjectStr next = iterator.next();
			System.out.println("URL 1 : " + next.getIRIStrEnt1());
			System.out.println("URL 2 : " + next.getIRIStrEnt2());
			System.out.println("Structural Mappings : " + next.getStructuralConfidenceMapping());
			System.out.println("Type Mapping : " + next.getTypeOfMapping());
			System.out.println("Confidence : " + next.getConfidence());
			System.out.println("lexicalConfidnce:" + next.getLexicalConfidenceMapping());
			System.out.println("property:" + next.isDataPropertyMapping());

			/*
				 * Extracting Resource and Property from Mapped Ontologies and Adding them to
				 * Jena Model to create an output file
				 */
			Resource resource = model.createResource(next.getIRIStrEnt1());
			Property related = model.createProperty("related To");
			resource.addProperty(related, next.getIRIStrEnt2());
		}

		System.out.println("Number of mappings computed by LogMap: " + logmap2_mappings.size());

		// Writing the compared ontologies to a local file.
		try (OutputStream out = new FileOutputStream("MappingOutput.ttl")) {
			model.write(out, "TURTLE");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// create an empty Model
		Model model2 = ModelFactory.createDefaultModel();
		return List.of(model2);
	}

	private void evaluationFun() {
		// TODO Auto-generated method stub

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
