package edu.cornell.mannlib.vitro.webapp.dao.jena;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.Lock;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import edu.cornell.mannlib.vitro.webapp.beans.Individual;
import edu.cornell.mannlib.vitro.webapp.beans.IndividualImpl;
import edu.cornell.mannlib.vitro.webapp.beans.ObjectProperty;
import edu.cornell.mannlib.vitro.webapp.beans.ObjectPropertyStatement;
import edu.cornell.mannlib.vitro.webapp.beans.ObjectPropertyStatementImpl;
import edu.cornell.mannlib.vitro.webapp.dao.IndividualDao;
import edu.cornell.mannlib.vitro.webapp.dao.VitroVocabulary;
import edu.cornell.mannlib.vitro.webapp.dao.jena.IndividualSDB.IndividualNotFoundException;
import edu.cornell.mannlib.vitro.webapp.dao.jena.WebappDaoFactorySDB.SDBDatasetMode;
import edu.cornell.mannlib.vitro.webapp.filestorage.model.ImageInfo;
import edu.cornell.mannlib.vitro.webapp.modelaccess.ContextModelAccess;
import edu.cornell.mannlib.vitro.webapp.modelaccess.ModelAccess;
import edu.cornell.mannlib.vitro.webapp.modelaccess.ModelNames;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.ResultSetConsumer;


public class IndividualBufferedSDB extends IndividualSDB {

private String indvGraphSparqlQuery=
        "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#> \n"
        + "PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>\n"
        + "PREFIX owl:      <http://www.w3.org/2002/07/owl#>\n"
        + "PREFIX c4o:      <http://purl.org/spar/c4o/>\n"
        + "PREFIX cito:     <http://purl.org/spar/cito/>\n"
        + "PREFIX event:    <http://purl.org/NET/c4dm/event.owl#>\n"
        + "PREFIX fabio:    <http://purl.org/spar/fabio/>\n"
        + "PREFIX foaf:     <http://xmlns.com/foaf/0.1/>\n"
        + "PREFIX dcterms:  <http://purl.org/dc/terms/>\n"
        + "PREFIX vann:     <http://purl.org/vocab/vann/>\n"
        + "PREFIX swo:      <http://www.ebi.ac.uk/efo/swo/>\n"
        + "PREFIX obo:      <http://purl.obolibrary.org/obo/>\n"
        + "PREFIX bibo:     <http://purl.org/ontology/bibo/>\n"
        + "PREFIX geo:      <http://aims.fao.org/aos/geopolitical.owl#>\n"
        + "PREFIX ocresd:   <http://purl.org/net/OCRe/study_design.owl#>\n"
        + "PREFIX ocrer:    <http://purl.org/net/OCRe/research.owl#>\n"
        + "PREFIX ro:       <http://purl.obolibrary.org/obo/ro.owl#>\n"
        + "PREFIX skos:     <http://www.w3.org/2004/02/skos/core#>\n"
        + "PREFIX ocresst:  <http://purl.org/net/OCRe/statistics.owl#>\n"
        + "PREFIX ocresp:   <http://purl.org/net/OCRe/study_protocol.owl#>\n"
        + "PREFIX vcard:    <http://www.w3.org/2006/vcard/ns#>\n"
        + "PREFIX vitro:    <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>\n"
        + "PREFIX scires:   <http://vivoweb.org/ontology/scientific-research#>\n"
        + "PREFIX vivocore: <http://vivoweb.org/ontology/core#>\n"
        + "PREFIX vivoweb:  <http://vivoweb.org/ontology#>\n"
        + "PREFIX sample:   <http://localhost:8080/vivo_i18n/individual/>\n"
        + "PREFIX demo-data: <http://vivo-demo.uqam.ca/individual/>\n"
        + "PREFIX vitrofrca: <http://vivoweb.org/ontology/vitroAnnotfr_CA#>\n"
        + "PREFIX vitro-public: <http://vitro.mannlib.cornell.edu/ns/vitro/public#>\n"
        + "CONSTRUCT {\n"
        + "     ?uri ?p ?o .\n"
        + "     ?mainImage ?p_mainImage ?o_mainImage .\n"
        + "     ?thumbnailImage ?p_thumbnailImage ?o_thumbnailImage .\n"
        + "     ?vIndividual ?p_vIndividual ?o_vIndividual . \n"
        + "     ?o_vIndividual ?po_vIndividual ?oo_vIndividual .\n"
        + "}\n"
        + "where {\n"
        + "    BIND ( <__INDIVIDUAL_IRI__> AS ?uri) .\n"
        + "    ?uri ?p ?o .\n"
        + "    FILTER ( ! STRENDS( STR(?p), \"hasResearchArea\") ) .\n"
        + "    FILTER ( ! STRENDS( STR(?p), \"relatedBy\") ) .\n"
        + "    FILTER ( ! STRENDS( STR(?p), \"researchAreaOf\") ) .\n"
        + "    FILTER ( ! STRENDS( STR(?p), \"type\") ) .\n"
        + "    OPTIONAL {\n"
        + "        ?uri vitro-public:mainImage ?mainImage .\n"
        + "        ?mainImage ?p_mainImage ?o_mainImage .\n"
        + "        ?mainImage  vitro-public:thumbnailImage ?thumbnailImage .\n"
        + "        ?thumbnailImage ?p_thumbnailImage ?o_thumbnailImage .\n"
        + "    }\n"
        + "    OPTIONAL {\n"
        + "        ?uri  obo:ARG_2000028 ?vIndividual .  \n"
        + "        ?vIndividual ?p_vIndividual ?o_vIndividual . \n"
        + "        ?o_vIndividual ?po_vIndividual ?oo_vIndividual .\n"
        + "        FILTER ( STRENDS( STR(?p_vIndividual), \"hasTitle\") ) .\n"
        + "        FILTER ( ! STRENDS( STR(?p_vIndividual), \"type\") ) .\n"
        + "        FILTER ( ! STRENDS( STR(?po_vIndividual), \"type\") ) .\n"
        + "    }\n"
        + "}";

//    private OntModel _buffOntModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
    private static final Log log = LogFactory.getLog(IndividualBufferedSDB.class.getName());    
    private OntModel _tboxModel;
    private List<String> _prefLangs;
    private Resource _individualJenaResource;
    private IndividualDaoJena _individualDaoJena;
    private IndividualJena _individualJena;
    private WebappDaoFactoryJena _jenaDaoFact;

    public IndividualBufferedSDB(String individualURI, IndividualDaoSDB individualDaoSDB,
            SDBDatasetMode datasetMode, WebappDaoFactorySDB wadf)
            throws edu.cornell.mannlib.vitro.webapp.dao.jena.IndividualSDB.IndividualNotFoundException {
        super(individualURI, individualDaoSDB.getDwf(), datasetMode, wadf, true);
        
        _tboxModel = individualDaoSDB.getOntModelSelector().getTBoxModel();
        if (LogManager.getRootLogger().getLevel() == Level.DEBUG) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            RDFDataMgr.write(stream, _tboxModel, Lang.TURTLE);
            log.debug(stream);
        }
    }
    public Model populateIndividualBufferModel(String individualUri) {
        if (!_buffOntModel.isEmpty())
            return _buffOntModel;
        try {
            log.info("Loading the buffer model of :" +individualUri);
            _prefLangs = webappDaoFactory.config.getPreferredLanguages();
            _buffOntModel.getLock().enterCriticalSection(Lock.READ);
            _individualJenaResource = ResourceFactory.createResource(individualURI);
            String _query = indvGraphSparqlQuery.replace("__INDIVIDUAL_IRI__", individualUri);
            log.debug(_query);
            webappDaoFactory.getRDFService().sparqlConstructQuery(_query, _buffOntModel, false);
            if (LogManager.getRootLogger().getLevel() == Level.DEBUG) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                RDFDataMgr.write(stream, _buffOntModel, Lang.TURTLE);
                log.debug(stream);
            }
            _buffOntModel.add(_tboxModel);
            _jenaDaoFact = (new WebappDaoFactoryJena(_buffOntModel));
            _individualJena = new IndividualJena(_buffOntModel.getOntResource(individualUri), _jenaDaoFact);
            log.debug(_individualDaoJena);
        } catch (RDFServiceException e) {
            e.printStackTrace();
        } finally {
            _buffOntModel.getLock().leaveCriticalSection();
        }
        return _buffOntModel;
    }
    private Model populateIndividualBufferModel() {
        return populateIndividualBufferModel(this.individualURI);
    }

    public List<String> getMostSpecificTypeURIs() {
        populateIndividualBufferModel();
        return _individualJena.getMostSpecificTypeURIs();
    }
    
//    private Property FS_DOWNLOAD_LOCATION_PROP=ResourceFactory.createProperty(VitroVocabulary.FS_DOWNLOAD_LOCATION);
//    private Property FS_THUMBNAIL_IMAGE_PROP=ResourceFactory.createProperty(VitroVocabulary.FS_THUMBNAIL_IMAGE);
    public String getMainImageUri() {
        populateIndividualBufferModel();
        return _individualJena.getMainImageUri();
//        List<Statement> stmts = getBuffOntModel().listStatements((Resource) null, FS_THUMBNAIL_IMAGE_PROP, (RDFNode) null).toList();
//        return stmts.get(0).getObject().asResource().getURI();
    }

    public String getImageUrl() {
        populateIndividualBufferModel();
        return _individualJena.getImageUrl();
    }
    
    public String getRdfsLabel() {
        populateIndividualBufferModel();
        return _individualJena.getRdfsLabel();
    }    
    public String getThumbUrl() {
        populateIndividualBufferModel();
        return _individualJena.getThumbUrl();
//        populateIndividualBufferModel();
//        List<Statement> stmts = getBuffOntModel().listStatements((Resource) null, FS_THUMBNAIL_IMAGE_PROP, (RDFNode) null).toList();
//        return stmts.get(0).getObject().asResource().getURI();
    }
    public OntModel getBuffOntModel() {
        return this._buffOntModel;
    }
}
