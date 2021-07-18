
package org.aksw.deer.plugin.example;

import org.aksw.deer.enrichments.AbstractParameterizedEnrichmentOperator;
import org.aksw.deer.vocabulary.DEER;
import org.aksw.faraday_cage.engine.ValidatableParameterMap;
import org.apache.jena.rdf.model.*;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Extension
public class ExampleEnrichmentOperator extends AbstractParameterizedEnrichmentOperator {

	private static final Logger logger = LoggerFactory.getLogger(ExampleEnrichmentOperator.class);

	public static Property NAME = DEER.property("name");

	@Override
	public ValidatableParameterMap createParameterMap() {
		return ValidatableParameterMap.builder().declareProperty(NAME)
				.declareValidationShape(getValidationModelFor(ExampleEnrichmentOperator.class)).build();
	}

	@Override
	protected List<Model> safeApply(List<Model> models) {
		Model model = models.get(0);
		System.out.println(" --Krishna-- model --------------------Inside ExampleEnrichmentOperator--------------------" + model);
		model.setNsPrefix("deer", DEER.NS);
		Resource resource = model.createResource(model.expandPrefix("deer:examplePlugin"));
		Property property = model.createProperty(model.expandPrefix("deer:says"));
		String recipient = getParameterMap().getOptional(NAME).map(RDFNode::asLiteral).map(Literal::getString)
				.orElse("World");
		logger.info("Greeting {}...", recipient);
		model.add(resource, property, "Hello " + recipient + "!");
		return List.of(model);
	}
}
