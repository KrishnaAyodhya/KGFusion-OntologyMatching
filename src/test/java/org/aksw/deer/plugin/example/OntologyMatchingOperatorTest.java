
package org.aksw.deer.plugin.example;

import java.util.List;
import java.util.Objects;

import org.aksw.deer.Deer;
import org.aksw.faraday_cage.engine.CompiledExecutionGraph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.junit.Test;
import org.pf4j.DefaultPluginManager;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

public class OntologyMatchingOperatorTest {

	@Test
	public void testFun() throws OWLOntologyCreationException {

		OntologyMatchingOperator refObj = new OntologyMatchingOperator();
		refObj.initPluginId(ResourceFactory.createResource("urn:ontology-matching-operator"));
		refObj.initParameters(refObj.createParameterMap().init());
		List<Model> res = refObj.safeApply(List.of(ModelFactory.createDefaultModel()));
	}

	@Test
	public void testConfiguration() {
		String url = Objects
				.requireNonNull(OntologyMatchingOperatorTest.class.getClassLoader().getResource("configuration.ttl"))
				.toExternalForm();
		Model configurationModel = ModelFactory.createDefaultModel().read(url);
		CompiledExecutionGraph executionGraph = Deer.getExecutionContext(new DefaultPluginManager())
				.compile(configurationModel);
		executionGraph.run();
		executionGraph.join();
	}

}