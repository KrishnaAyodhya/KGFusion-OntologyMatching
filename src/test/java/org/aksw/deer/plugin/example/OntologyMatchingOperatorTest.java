
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

public class OntologyMatchingOperatorTest {
	
	@Test
	public void abcTest()
	{
		OntologyMatchingOperator refObj = new OntologyMatchingOperator();
		Model a =ModelFactory.createDefaultModel();
		
		refObj.safeApply(null);
		
		
	}
}
