
package org.aksw.deer.plugin.example;

import org.aksw.deer.Deer;
import org.aksw.deer.vocabulary.DEER;
import org.aksw.faraday_cage.engine.CompiledExecutionGraph;
import org.aksw.faraday_cage.engine.ValidatableParameterMap;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.junit.Test;
import org.pf4j.DefaultPluginManager;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertTrue;

public class ExampleEnrichmentOperatorTest {
/*
	@Test
	public void testParameterless() {
		Model expected = ModelFactory.createDefaultModel();
		expected.add(DEER.resource("examplePlugin"), DEER.property("says"),
				ResourceFactory.createPlainLiteral("Hello World!"));
		ExampleEnrichmentOperator eeo = new ExampleEnrichmentOperator();
		eeo.initPluginId(ResourceFactory.createResource("urn:example-enrichment-operator"));
		eeo.initDegrees(1, 1);
		ValidatableParameterMap params = eeo.createParameterMap().init();
		eeo.initParameters(params);
		Model result = eeo.safeApply(List.of(ModelFactory.createDefaultModel())).get(0);
		assertTrue(
				"The empty model enriched by the example enrichment operator should contain exactly"
						+ " one triple: 'deer:examplePlugin deer:says \"Hello World!\"'.",
				result.isIsomorphicWith(expected));
	}

	@Test
	public void testParameter() {
		Model expected = ModelFactory.createDefaultModel();
		expected.add(DEER.resource("examplePlugin"), DEER.property("says"),
				ResourceFactory.createPlainLiteral("Hello Students!"));
		ExampleEnrichmentOperator eeo = new ExampleEnrichmentOperator();
		eeo.initPluginId(ResourceFactory.createResource("urn:example-enrichment-operator"));
		eeo.initDegrees(1, 1);
		ValidatableParameterMap params = eeo.createParameterMap()
				.add(ExampleEnrichmentOperator.NAME, ResourceFactory.createStringLiteral("Students")).init();
		eeo.initParameters(params);
		Model result = eeo.safeApply(List.of(ModelFactory.createDefaultModel())).get(0);
		assertTrue(
				"The empty model enriched by the example enrichment operator should contain exactly"
						+ " one triple: 'deer:examplePlugin deer:says \"Hello Students!\"'.",
				result.isIsomorphicWith(expected));
	}

	@Test
	public void testConfiguration() {
		String url = Objects
				.requireNonNull(ExampleEnrichmentOperatorTest.class.getClassLoader().getResource("configuration.ttl"))
				.toExternalForm();
		Model configurationModel = ModelFactory.createDefaultModel().read(url);
		CompiledExecutionGraph executionGraph = Deer.getExecutionContext(new DefaultPluginManager())
				.compile(configurationModel);
		executionGraph.run();
		executionGraph.join();
	}
*/
}
