/* $This file is distributed under the terms of the license in /doc/license.txt$ */

package edu.cornell.mannlib.vitro.webapp.dao.jena;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import edu.cornell.mannlib.vitro.webapp.beans.VClass;
import edu.cornell.mannlib.vitro.webapp.dao.VitroVocabulary;
import edu.cornell.mannlib.vitro.webapp.dao.WebappDaoFactory;

public class JenaModelUtils {
	
	private static final Log log = LogFactory.getLog(JenaModelUtils.class.getName());

	private static final Set<String>  nonIndividualTypeURIs ;
	
	static {
		nonIndividualTypeURIs = new HashSet<String>();
		nonIndividualTypeURIs.add(OWL.Class.getURI());
		nonIndividualTypeURIs.add(OWL.Restriction.getURI());
		nonIndividualTypeURIs.add(OWL.ObjectProperty.getURI());
		nonIndividualTypeURIs.add(OWL.DatatypeProperty.getURI());
		nonIndividualTypeURIs.add(OWL.AnnotationProperty.getURI());
		nonIndividualTypeURIs.add(OWL.Ontology.getURI());
		nonIndividualTypeURIs.add(RDFS.Class.getURI());
		nonIndividualTypeURIs.add(RDF.Property.getURI());
	}
	
	public synchronized static void makeClassGroupsFromRootClasses(WebappDaoFactory wadf, Model ontModel) {
	    makeClassGroupsFromRootClasses(wadf, ontModel, ontModel);
	}
	
	public synchronized static OntModel makeClassGroupsFromRootClasses(WebappDaoFactory wadf, Model baseModel, Model vitroInternalsSubmodel) {		
		OntModel ontModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM,baseModel);
		OntModel modelForClassgroups = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);
		SimpleOntModelSelector oms = new SimpleOntModelSelector();
		oms.setTBoxModel(ontModel);
		oms.setApplicationMetadataModel(modelForClassgroups);
		WebappDaoFactory myWebappDaoFactory = new WebappDaoFactoryJena(new SimpleOntModelSelector(ontModel),wadf.getDefaultNamespace(),null,null,null);
		OntModel tempModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);
	    Resource classGroupClass = ResourceFactory.createResource(VitroVocabulary.CLASSGROUP);
	    Property inClassGroupProperty = ResourceFactory.createProperty(VitroVocabulary.IN_CLASSGROUP);
	    ontModel.enterCriticalSection(Lock.READ);
	    try { 
    		try {	    			
        	    for (Iterator rootClassIt = myWebappDaoFactory.getVClassDao().getRootClasses().iterator(); rootClassIt.hasNext(); ) {
        	    	VClass rootClass = (VClass) rootClassIt.next();
        	    	Individual classGroup = tempModel.createIndividual(wadf.getDefaultNamespace()+"vitroClassGroup"+rootClass.getLocalName(), classGroupClass);
        	    	classGroup.addProperty(tempModel.getProperty(VitroVocabulary.DISPLAY_RANK_ANNOT),"50",XSDDatatype.XSDint);
        	    	classGroup.setLabel(rootClass.getName(),null);
        	    	OntClass rootClassOntClass = ontModel.getOntClass(rootClass.getURI());
        	    	tempModel.add(rootClassOntClass, inClassGroupProperty, classGroup);
        	    	for (Iterator childIt = myWebappDaoFactory.getVClassDao().getAllSubClassURIs(rootClass.getURI()).iterator(); childIt.hasNext(); ) {
        	    		String childURI = (String) childIt.next();
        	    		OntClass childClass = ontModel.getOntClass(childURI);
        	    		childClass.addProperty(inClassGroupProperty, classGroup);
        	    	}
        	    }
    		} catch (Exception e) {
    			log.error("Unable to create class groups automatically based on class hierarchy");
    		}
    		vitroInternalsSubmodel.enterCriticalSection(Lock.WRITE);
    		try {
    			vitroInternalsSubmodel.add(tempModel);
    		} finally {
    			vitroInternalsSubmodel.leaveCriticalSection();
    		}
	    } finally {
	    	ontModel.leaveCriticalSection();
	    }
	    return modelForClassgroups;
	}
	
private final OntModelSpec DEFAULT_ONT_MODEL_SPEC = OntModelSpec.OWL_MEM;
	
	public OntModel extractTBox( Model inputModel) {
	return extractTBox(inputModel, null);	
	}
	
	public OntModel extractTBox( Model inputModel, String namespace ) {
	    Dataset dataset = DatasetFactory.create(inputModel);
        return extractTBox( dataset, namespace, null );
	}
	
	public OntModel extractTBox( Dataset dataset, String namespace, String graphURI ) {
		OntModel tboxModel = ModelFactory.createOntologyModel(DEFAULT_ONT_MODEL_SPEC);
		
		List<String> queryStrList = new LinkedList<String>();
		
		// Use SPARQL DESCRIBE queries to extract the RDF for named ontology entities
		
		queryStrList.add( makeDescribeQueryStr( OWL.Class.getURI(), namespace, graphURI ) );
		queryStrList.add( makeDescribeQueryStr( OWL.ObjectProperty.getURI(), namespace, graphURI ) );
		queryStrList.add( makeDescribeQueryStr( OWL.DatatypeProperty.getURI(), namespace, graphURI ) );
		queryStrList.add( makeDescribeQueryStr( OWL.AnnotationProperty.getURI(), namespace, graphURI ) );
		// if we're using to a hash namespace, the URI of the Ontology resource will be
		// that namespace minus the final hash mark.
		if ( namespace != null && namespace.endsWith("#") ) {
			queryStrList.add( makeDescribeQueryStr( OWL.Ontology.getURI(), namespace.substring(0,namespace.length()-2), graphURI ) );	
		} else {
			queryStrList.add( makeDescribeQueryStr( OWL.Ontology.getURI(), namespace, graphURI ) );
		}
		
		// Perform the SPARQL DESCRIBEs
		for ( String queryStr : queryStrList ) {
			Query tboxSparqlQuery = QueryFactory.create(queryStr);
			QueryExecution qe = QueryExecutionFactory.create(tboxSparqlQuery,dataset);
			try {
				dataset.getLock().enterCriticalSection(Lock.READ);
				qe.execDescribe(tboxModel);
			} finally {
				dataset.getLock().leaveCriticalSection();
			}
		}
		
		return tboxModel;
		
	}
	
	private String makeDescribeQueryStr( String typeURI, String namespace ) {
	    return makeDescribeQueryStr( typeURI, namespace, null );
	} 	
		
	private String makeDescribeQueryStr( String typeURI, String namespace, String graphURI ) {
		
		StringBuffer describeQueryStrBuff = new StringBuffer() 
			.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n")
			.append("PREFIX afn: <http://jena.hpl.hp.com/ARQ/function#> \n")
			.append("DESCRIBE ?res WHERE { \n");
			if (graphURI != null) {
				describeQueryStrBuff
				.append("GRAPH " + graphURI + "{ \n");
			}
			describeQueryStrBuff
			.append("    ?res rdf:type <").append(typeURI).append("> . \n");
			
			describeQueryStrBuff
			.append("    FILTER (!isBlank(?res)) \n");
		
		if (namespace == null) {
			// exclude resources in the Vitro internal namespace or in the 
			// OWL namespace, but allow all others
			describeQueryStrBuff
			//.append("    FILTER (afn:namespace(?res) != \"")
			//.append("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
			//.append("\") \n")
			.append("    FILTER (afn:namespace(?res) != \"")
			.append("http://www.w3.org/2002/07/owl#")
			.append("\") \n")
			//.append("    FILTER (?res != <")
			//.append("http://vitro.mannlib.cornell.edu/ns/vitro/0.7")
			//.append(">) \n")
			.append("    FILTER (?res != <")
			.append("http://www.w3.org/2002/07/owl")
			.append(">) \n");
		} else {
			// limit resources to those in the supplied namespace
			describeQueryStrBuff
			.append("    FILTER (afn:namespace(?res) = \"")
			.append(namespace)
			.append("\") \n");	
		}
		if (graphURI != null) {
			describeQueryStrBuff
			.append("} \n");
		}
			
		describeQueryStrBuff.append("} \n");
		
		return describeQueryStrBuff.toString();
		
	}
	
	public Model extractABox(Model inputModel){
		Dataset dataset = DatasetFactory.create(inputModel);
	    return extractABox(dataset, null, null);
	}
	
	public Model extractABox( Dataset unionDataset, Dataset baseOrInfDataset, String graphURI ) {
		
		Model aboxModel = ModelFactory.createDefaultModel();

		// iterate through all classes and DESCRIBE each of their instances
		// Note that this could be simplified if we knew that the model was a
		// reasoning model: we could then simply describe all instances of 
		// owl:Thing.

		//OntModel ontModel = ( inputModel instanceof OntModel ) 
		//? (OntModel)inputModel
		//: ModelFactory.createOntologyModel( DEFAULT_ONT_MODEL_SPEC, inputModel );
		OntModel ontModel = extractTBox(unionDataset, null, graphURI);
		
	
		try {
			ontModel.enterCriticalSection(Lock.READ);
			Iterator classIt = ontModel.listNamedClasses();
			QueryExecution qe = null;
			while ( classIt.hasNext() ) {
				
				OntClass ontClass = (OntClass) classIt.next();
				//if ( !(ontClass.getNameSpace().startsWith(OWL.getURI()) )  
				// && !(ontClass.getNameSpace().startsWith(VitroVocabulary.vitroURI))	) {
			 if(!(ontClass.getNameSpace().startsWith(OWL.getURI()))){
					
					String queryStr = makeDescribeQueryStr( ontClass.getURI(), null, graphURI );
					
					Query aboxSparqlQuery = QueryFactory.create(queryStr);
					if(baseOrInfDataset != null){
						qe = QueryExecutionFactory.create(aboxSparqlQuery,baseOrInfDataset);
					}
					else{
						qe = QueryExecutionFactory.create(aboxSparqlQuery,unionDataset);
					}
					if(baseOrInfDataset != null){
						try {
							baseOrInfDataset.getLock().enterCriticalSection(Lock.READ);
							qe.execDescribe(aboxModel); // puts the statements about each resource into aboxModel.
						} finally {
							baseOrInfDataset.getLock().leaveCriticalSection();
						}
					}
					else{
						try {
							unionDataset.getLock().enterCriticalSection(Lock.READ);
							qe.execDescribe(aboxModel); // puts the statements about each resource into aboxModel.
						} finally {
							unionDataset.getLock().leaveCriticalSection();
						}
					}
					
				}
			}
		} finally {
			ontModel.leaveCriticalSection();
		}
		
		return aboxModel;
		
	}
	
	public Model extractUserAccountsData(Model inputModel) {
		
		Model userAccountsModel = ModelFactory.createDefaultModel();
		
		String queryStr = makeDescribeQueryStr( VitroVocabulary.USER, null );
		
		Query usersSparqlQuery = QueryFactory.create(queryStr);
		QueryExecution qe = QueryExecutionFactory.create(usersSparqlQuery,inputModel);
		try {
			inputModel.enterCriticalSection(Lock.READ);
			qe.execDescribe(userAccountsModel);
		} finally {
			inputModel.leaveCriticalSection();
		}
		
		return userAccountsModel;
		
	}
	
}
