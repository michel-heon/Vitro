/* $This file is distributed under the terms of the license in LICENSE$ */

package edu.cornell.mannlib.vitro.webapp.controller.json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.cornell.mannlib.vitro.webapp.web.templatemodels.individual.IndividualTemplateModelBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import edu.cornell.mannlib.vitro.webapp.beans.Individual;
import edu.cornell.mannlib.vitro.webapp.config.ConfigurationProperties;
import edu.cornell.mannlib.vitro.webapp.controller.VitroRequest;
import edu.cornell.mannlib.vitro.webapp.dao.IndividualDao;
import edu.cornell.mannlib.vitro.webapp.services.shortview.ShortViewService;
import edu.cornell.mannlib.vitro.webapp.services.shortview.ShortViewService.ShortViewContext;
import edu.cornell.mannlib.vitro.webapp.services.shortview.ShortViewServiceSetup;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Does a search for individuals, and uses the short view to render each of
 * the results.
 */
public class GetRenderedSearchIndividualsByVClass extends GetSearchIndividualsByVClasses {
    /** This value is set in the runtime.property file **/
    public static final String RENDERED_SEARCH_INDIVIDUAL_PERPAGE = "rendered.search.individual.perpage";
	static final String RENDERED_SEARCH_INDIVIDUAL_ANALYSE_PROCESS = "rendered.search.individual.analyseProcess";
    static final String RENDERED_SEARCH_INDIVIDUAL_PARALLEL_PROCESSING = "rendered.search.individual.parallelProcessing";
    static final String RENDERED_SEARCH_INDIVIDUAL_BUFFERED = "rendered.search.individual.buffered";
    private static final Log log = LogFactory.getLog(GetRenderedSearchIndividualsByVClass.class);
    private boolean isBufferedSearchIndividual = false;
    private boolean isParallelProcessing = false;
    private boolean isAnalyseProcess = false;
    /*
     * Approximately 50 SPARQL queries are required to process each individual. 
     * The value of this variable has a major impact on the refresh 
     * performance of the front pages of each tab, especially on person and organization
     *
     */
    private static int INDIVIDUALS_PER_PAGE = 0; 
    public static int getIndividualsPerPage(VitroRequest vreq) {
        if (INDIVIDUALS_PER_PAGE != 0 ) return INDIVIDUALS_PER_PAGE;
        ConfigurationProperties prop = ConfigurationProperties.getBean(vreq);
        INDIVIDUALS_PER_PAGE = Integer.valueOf(prop.getProperty(RENDERED_SEARCH_INDIVIDUAL_PERPAGE,"30"));
        return INDIVIDUALS_PER_PAGE;
    }

    protected GetRenderedSearchIndividualsByVClass(VitroRequest vreq) {
        super(vreq);
        String propValue=null;
        ConfigurationProperties prop = ConfigurationProperties.getBean(vreq);
        try {
            propValue=prop.getProperty(RENDERED_SEARCH_INDIVIDUAL_BUFFERED,"false");
            isBufferedSearchIndividual = Boolean.valueOf(propValue);
            log.debug("isBufferedSearchIndividual = "+isBufferedSearchIndividual());
        } catch (Exception e) {
            log.warn("There's a problem with the (\"+RENDERED_SEARCH_INDIVIDUAL_BUFFERED+\") property and the value ("+propValue+") obtained. A boolean is expected"); 
        }
        try {
            propValue=prop.getProperty(RENDERED_SEARCH_INDIVIDUAL_PARALLEL_PROCESSING,"false");
            isParallelProcessing = Boolean.valueOf(propValue);
            log.debug("isParallelProcessing = "+isParallelProcessing());
        } catch (Exception e) {
            log.warn("There's a problem with the (\"+RENDERED_SEARCH_INDIVIDUAL_PARALLEL_PROCESSING+\") property and the value ("+propValue+") obtained. A boolean is expected"); 
        }
        try {
            propValue=prop.getProperty(RENDERED_SEARCH_INDIVIDUAL_ANALYSE_PROCESS,"false");
            isAnalyseProcess = Boolean.valueOf(propValue);
            log.debug("isAnalyseProcess = "+isAnalyseProcess());
        } catch (Exception e) {
            log.warn("There's a problem with the (\"+RENDERED_SEARCH_INDIVIDUAL_ANALYSE_PROCESS+\") property and the value ("+propValue+") obtained. A boolean is expected"); 
        }
    }

	/**
	 * Search for individuals by VClass or VClasses in the case of multiple parameters. The class URI(s) and the paging
	 * information are in the request parameters.
	 */
	@Override
	protected ObjectNode process() throws Exception {
		ObjectNode rObj = null;

		//This gets the first vclass value and sets that as display type
		List<String> vclassIds = super.getVclassIds(vreq);
		String vclassId = null;
		if(vclassIds.size() > 1) {
			//This ensures the second class instead of the first
			//This is a temporary fix in cases where institutional internal classes are being sent in
			//and thus the second class is the actual class with display information associated
			vclassId = vclassIds.get(1);
		} else {
			vclassId = vclassIds.get(0);
		}
		vreq.setAttribute("displayType", vclassId);

		//This will get all the solr individuals by VClass (if one value) or the intersection
		//i.e. individuals that have all the types for the different vclasses entered
		rObj = super.process();
		addShortViewRenderings(rObj);
		return rObj;
	}

//    /**
//     * Look through the return object. For each individual, render the short view
//     * and insert the resulting HTML into the object.
//     * ORIGINAL CODE 
//     * This code segment is never called. 
//     * It is kept for the time being for performance comparison tests. 
//     * It should be deleted when PR is accepted.
//     */
//	private void addShortViewRenderings_ORIGINAL(ObjectNode rObj) {
//		ArrayNode individuals = (ArrayNode) rObj.get("individuals");
//		String vclassName = rObj.get("vclass").get("name").asText();
//		for (int i = 0; i < individuals.size(); i++) {
//			ObjectNode individual = (ObjectNode) individuals.get(i);
//			individual.put("shortViewHtml",
//					renderShortView(individual.get("URI").asText(), vclassName));
//		}
//	}
//    /**
//     * Look through the return object. For each individual, render the short view
//     * and insert the resulting HTML into the object.
//     * The use of multi treading allows to submit the requests and the parallel 
//     * processing of the elements necessary for the html objects of the page. 
//     * Parallel processing allows maximum network bandwidth utilization.
//     */
//    private void addShortViewRenderings(ObjectNode rObj) {
//
//        ArrayNode individuals = (ArrayNode) rObj.get("individuals");
//        String vclassName = rObj.get("vclass").get("name").asText();
//        int indvSize = individuals.size();
//        ExecutorService es = Executors.newFixedThreadPool(indvSize);
//        for (int i = 0; i < indvSize; i++) {
//            ObjectNode individual = (ObjectNode) individuals.get(i);
//               ProcessIndividual pi = new ProcessIndividual();
//               pi.setIndividual(individual);
//               pi.setVclassName(vclassName);
//               es.execute(pi);
//        }
//        es.shutdown();
//        try {
//            while(!es.awaitTermination(250, TimeUnit.MILLISECONDS)){
//            }
//        } catch (InterruptedException e1) {
//        }
//    }
//    /*
//     * The runnable class that executes the renderShortView 
//     */
//    private class ProcessIndividual implements Runnable {
//        private ObjectNode individual = null;
//        private String vclassName;
//        public String getVclassName() {
//            return vclassName;
//        }
//        public void setVclassName(String vclassName) {
//            this.vclassName = vclassName;
//        }
//        // Method
//        public void run() {
//            individual.put("shortViewHtml", renderShortView(individual.get("URI").asText(), vclassName));
//        }
//        public void setIndividual(ObjectNode individual) {
//            this.individual = individual;
//        }
//    }
//    
//    private String renderShortView(String individualUri, String vclassName) {

    private void analyserLog(String message) {
        if (isAnalyseProcess()) log.info("ANLYSER: "+message);
    }
    /**
     * Look through the return object. For each individual, render the short
     * view and insert the resulting HTML into the object.
     */
    private void addShortViewRenderings(ObjectNode rObj) {
        if (isBufferedSearchIndividual) {
            analyserLog("individual searches are buffered");
        } else {
            analyserLog("individual searches are NOT-buffered");
        }
        if (this.isParallelProcessing)
            addShortViewRenderingsParallelMode(rObj);
        else 
            addShortViewRenderingsSerialMode(rObj);
    }   

    private void addShortViewRenderingsSerialMode(ObjectNode rObj) {
//        LogManager.getRootLogger().setLevel(Level.DEBUG);
        ArrayNode individuals = (ArrayNode) rObj.get("individuals");
        String vclassName = rObj.get("vclass").get("name").asText();
        Instant d1=null;
        if (isAnalyseProcess()) d1 = Instant.now();
        int totalIndv = individuals.size();
        for (int i = 0; i < individuals.size(); i++) {
            ObjectNode individual = (ObjectNode) individuals.get(i);
            Instant t1 = null;
            Instant t2 = null;
            if (isAnalyseProcess()) t1 = Instant.now();
            individual.put("shortViewHtml", renderShortView(individual.get("URI").asText(), vclassName));
            if (isAnalyseProcess()) {
                t2 = Instant.now();
                long totalTime = ChronoUnit.MILLIS.between(t1, t2);
                if (isAnalyseProcess()) {
                    try {
                        analyserLog("The treatment at (" + t2 +") for (" + individual.get("URI").asText()+") took "+ totalTime/1000.0 + " seconds");
                    } catch (Exception e) { }
                }
            }
        }
        if (isAnalyseProcess()) { 
            Instant d2 = Instant.now();
            long totalTime = ChronoUnit.MILLIS.between(d1, d2);
            try {
                analyserLog("total indv:(" + totalIndv + 
                        ") total time (sec.):(" + totalTime / 1000.0 + 
                        ") avrg time (sec.): " + (totalTime / totalIndv) / 1000.0);
            } catch (Exception e) {
                analyserLog("total indv:(" + totalIndv + 
                        ") total time (sec.):(" + totalTime / 1000.0 +") ");
            }
        }
    }
    
    private void addShortViewRenderingsParallelMode(ObjectNode rObj) {
        ArrayNode individuals = (ArrayNode) rObj.get("individuals");
        String vclassName = rObj.get("vclass").get("name").asText();
        int indvSize = individuals.size();
        ExecutorService es = Executors.newFixedThreadPool(indvSize);
        Instant d1 = null;
        if (isAnalyseProcess())  d1 = Instant.now();
        for (int i = 0; i < indvSize; i++) {
               ObjectNode individual = (ObjectNode) individuals.get(i);
               ProcessIndividual pi = new ProcessIndividual();
               pi.setIndividual(individual);
               pi.setVclassName(vclassName);
               es.execute(pi);
        }
        es.shutdown();
        try {
            while(!es.awaitTermination(10, TimeUnit.MILLISECONDS)){
            }
        } catch (InterruptedException e1) {
        }
        if (isAnalyseProcess()) {
            Instant d2 = Instant.now();
            long totalTime = ChronoUnit.MILLIS.between(d1, d2);
            try {
                analyserLog("total indv:(" + indvSize + 
                        ") total time (sec.):(" + totalTime / 1000.0 + 
                        ") avrg time (sec.): " + (totalTime / indvSize) / 1000.0);
            } catch (Exception e) {
                analyserLog("total indv:(" + indvSize + 
                        ") total time (sec.):(" + totalTime / 1000.0 +") ");
            }
        }
    }
    /*
     * The runnable class that executes the renderShortView 
     */
    private class ProcessIndividual implements Runnable {
        private ObjectNode individual = null;
        private String vclassName;
        public String getVclassName() {
            return vclassName;
        }
        public void setVclassName(String vclassName) {
            this.vclassName = vclassName;
        }
        // Method
        public void run() {
            individual.put("shortViewHtml", renderShortView(individual.get("URI").asText(), vclassName));
        }
        public void setIndividual(ObjectNode individual) {
            this.individual = individual;
        }
    }

    protected String renderShortView(String individualUri, String vclassName) {
        analyserLog("renderShortView START :" + individualUri);
        IndividualDao iDao;
        // Instant t1 = Instant.now();
        if (isBufferedSearchIndividual()) {
            iDao = vreq.getBufferedIndividualWebappDaoFactory().getIndividualDao();
        } else {
            iDao = vreq.getWebappDaoFactory().getIndividualDao();
        }
        Individual individual = iDao.getIndividualByURI(individualUri);
        Map<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("individual", IndividualTemplateModelBuilder.build(individual, vreq));
        modelMap.put("vclass", vclassName);
        ShortViewService svs = ShortViewServiceSetup.getService(ctx);
        String rsv = svs.renderShortView(individual, ShortViewContext.BROWSE, modelMap, vreq);
        analyserLog("renderShortView END :" + individualUri);
        return rsv;
    }

    /**
     * @return the isBufferedSearchIndividual
     */
    public boolean isBufferedSearchIndividual() {
        return isBufferedSearchIndividual;
    }

    /**
     * @param isBufferedSearchIndividual the isBufferedSearchIndividual to set
     */
    public void setBufferedSearchIndividual(boolean isBufferedSearchIndividual) {
        this.isBufferedSearchIndividual = isBufferedSearchIndividual;
    }

    /**
     * @return the isParallelProcessing
     */
    public boolean isParallelProcessing() {
        return isParallelProcessing;
    }

    /**
     * @param isParallelProcessing the isParallelProcessing to set
     */
    public void setParallelProcessing(boolean isParallelProcessing) {
        this.isParallelProcessing = isParallelProcessing;
    }

    /**
     * @return the isAnalyseProcess
     */
    public boolean isAnalyseProcess() {
        return isAnalyseProcess;
    }

    /**
     * @param isAnalyseProcess the isAnalyseProcess to set
     */
    public void setAnalyseProcess(boolean isAnalyseProcess) {
        this.isAnalyseProcess = isAnalyseProcess;
    }
}
