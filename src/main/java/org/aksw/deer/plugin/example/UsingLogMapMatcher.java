package org.aksw.deer.plugin.example;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import uk.ac.ox.krr.logmap2.LogMap2_Matcher;
import uk.ac.ox.krr.logmap2.mappings.objects.MappingObjectStr;


/**
 * 
 * Example of using LogMap's matching facility 
 *  * 
 * @author Ernesto
 *
 */
public class UsingLogMapMatcher {
	

	
	
	OWLOntology onto1;
	OWLOntology onto2;

	OWLOntologyManager onto_manager;
	
	public UsingLogMapMatcher(){
		//Logger.getLogger(UsingLogMapMatcher.class.getName()).setLevel(Level.OFF);
		Logger.getRootLogger().setLevel(Level.OFF);
		
		try{
						
			String onto1_iri = "yagoo.ttl";
			String onto2_iri = "dbpedia.ttl";
			
			onto_manager = OWLManager.createOWLOntologyManager();
			
			onto1 = onto_manager.loadOntologyFromOntologyDocument(new File(onto1_iri));
			onto2 = onto_manager.loadOntologyFromOntologyDocument(new File(onto2_iri));
			
			
			
			
			//onto1 = onto_manager.loadOntology(IRI.create(onto1_iri));
			//onto2 = onto_manager.loadOntology(IRI.create(onto2_iri));
			
			
			LogMap2_Matcher logmap2 = new LogMap2_Matcher(onto1,onto2);
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
			
			  Set<String> representativeLabelsForMappings =
			  logmap2.getRepresentativeLabelsForMappings(); System.out.println("Labels : "
			  + representativeLabelsForMappings);
			  
			  
			  
			  
			 
			//Optionally LogMap also accepts the IRI strings as input 
			//LogMap2_Matcher logmap2 = new LogMap2_Matcher(onto1_iri, onto2_iri);
			
			//Set of mappings computed my LogMap
			Set<MappingObjectStr> logmap2_mappings = logmap2.getLogmap2_Mappings();
			
			Iterator<MappingObjectStr> iterator = logmap2_mappings.iterator();
			
			while(iterator.hasNext()) {
				MappingObjectStr next = iterator.next();
				System.out.println("URL 1 : " + next.getIRIStrEnt1());
				System.out.println("URL 2 : " + next.getIRIStrEnt2());
				System.out.println("Structural Mappings : " + next.getStructuralConfidenceMapping());
				System.out.println("Type Mapping : " + next.getTypeOfMapping());
				System.out.println("Confidence : "+ next.getConfidence());
				System.out.println("lexicalConfidnce:" +next.getLexicalConfidenceMapping());
				System.out.println("property:" +  next.isDataPropertyMapping());
				
				
			}
			
			System.out.println("Number of mappings computed by LogMap: " + logmap2_mappings.size());
			
			
			/*
			 * Accessing mapping objects
			 *  
			for (MappingObjectStr mapping: logmap2_mappings){
				System.out.println("Mapping: ");
				System.out.println("\t"+ mapping.getIRIStrEnt1());
				System.out.println("\t"+ mapping.getIRIStrEnt2());
				System.out.println("\t"+ mapping.getConfidence());
				
				//MappingObjectStr.EQ or MappingObjectStr.SUB or MappingObjectStr.SUP
				System.out.println("\t"+ mapping.getMappingDirection()); //Utilities.EQ;
				
				//MappingObjectStr.CLASSES or MappingObjectStr.OBJECTPROPERTIES or MappingObjectStr.DATAPROPERTIES or MappingObjectStr.INSTANCES
				System.out.println("\t"+ mapping.getTypeOfMapping());
				
			}*/
			
		}
		catch (Exception e){
			e.printStackTrace();
		}
		
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		new UsingLogMapMatcher();

	}

}