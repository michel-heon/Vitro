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
	private static final String RENDERED_SEARCH_INDIVIDUAL_BUFFERED = "rendered.search.individual.buffered";
    private static final Log log = LogFactory
			.getLog(GetRenderedSearchIndividualsByVClass.class);
    private boolean isBufferedSearchIndividual = false;

	protected GetRenderedSearchIndividualsByVClass(VitroRequest vreq) {
		super(vreq);
		String propValue=null;
        try {
	        ConfigurationProperties prop = ConfigurationProperties.getBean(vreq);
	        propValue=prop.getProperty(RENDERED_SEARCH_INDIVIDUAL_BUFFERED,"false");
	        isBufferedSearchIndividual = Boolean.valueOf(propValue);
        } catch (Exception e) {
            log.warn("There's a problem with the (\"+RENDERED_SEARCH_INDIVIDUAL_BUFFERED+\") property and the value ("+propValue+") obtained. A boolean is expected"); 
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

	   /**
     * Look through the return object. For each individual, render the short
     * view and insert the resulting HTML into the object.
     */
    private void addShortViewRenderings(ObjectNode rObj) {
//        LogManager.getRootLogger().setLevel(Level.DEBUG);
        ArrayNode individuals = (ArrayNode) rObj.get("individuals");
        String vclassName = rObj.get("vclass").get("name").asText();
        Instant d1 = Instant.now();
        int totalIndv = individuals.size();
        for (int i = 0; i < individuals.size(); i++) {
            ObjectNode individual = (ObjectNode) individuals.get(i);
            Instant t1 = Instant.now();
            individual.put("shortViewHtml", renderShortView(individual.get("URI").asText(), vclassName));
            Instant t2 = Instant.now();
            long totalTime = ChronoUnit.MILLIS.between(t1, t2);
            log.info("ANALYSER: The treatment at (" + t2 +") for (" + individual.get("URI").asText()+") took "+ totalTime/1000.0 + " seconds");

        }
        Instant d2 = Instant.now();
        long totalTime = ChronoUnit.MILLIS.between(d1, d2);
//        LogManager.getRootLogger().setLevel(Level.INFO);
        try {
            log.info("ANALYSER: total indv:(" + totalIndv + 
                    ") total time (sec.):(" + totalTime / 1000.0 + 
                    ") avrg time (sec.): " + (totalTime / totalIndv) / 1000.0);           
        } catch (Exception e) {
            log.info("ANALYSER: total indv:(" + totalIndv + 
                    ") total time (sec.):(" + totalTime / 1000.0 +") ");           
        }
    }
    private void _addShortViewRenderings(ObjectNode rObj) {
        ArrayNode individuals = (ArrayNode) rObj.get("individuals");
        String vclassName = rObj.get("vclass").get("name").asText();
        int indvSize = individuals.size();
        ExecutorService es = Executors.newFixedThreadPool(indvSize);
        Instant d1 = Instant.now();
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
        Instant d2 = Instant.now();
        long totalTime = ChronoUnit.MILLIS.between(d1, d2);
        try {
            log.info("ANALYSER: total indv:(" + indvSize + 
                    ") total time (sec.):(" + totalTime / 1000.0 + 
                    ") avrg time (sec.): " + (totalTime / indvSize) / 1000.0);           
        } catch (Exception e) {
            log.info("ANALYSER: total indv:(" + indvSize + 
                    ") total time (sec.):(" + totalTime / 1000.0 +") ");           
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
//        LogManager.getRootLogger().setLevel(Level.DEBUG);
        log.debug("start :" + individualUri);
        IndividualDao iDao;
        // Instant t1 = Instant.now();
        if (isBufferedSearchIndividual) {
            iDao = vreq.getBufferedIndividualWebappDaoFactory().getIndividualDao();
            log.info("individual searches are buffered");
        } else {
            iDao = vreq.getWebappDaoFactory().getIndividualDao();
            log.info("individual searches are NOT-buffered");
        }
        // Instant t2 = Instant.now();
        Individual individual = iDao.getIndividualByURI(individualUri);
//        log.info("toString "+ individual);
//        Instant t3 = Instant.now();

        Map<String, Object> modelMap = new HashMap<String, Object>();
        modelMap.put("individual", IndividualTemplateModelBuilder.build(individual, vreq));
        modelMap.put("vclass", vclassName);
//        Instant t4 = Instant.now();

        ShortViewService svs = ShortViewServiceSetup.getService(ctx);

//        Instant t5 = Instant.now();
        String rsv = svs.renderShortView(individual, ShortViewContext.BROWSE, modelMap, vreq);
//        Instant t6 = Instant.now();
//        log.info("toString "+ individual);
//        log.info("ANALYSER: "+
//        " total-renderShortView="+ChronoUnit.MILLIS.between(t5,t6)/1000.0+ 
//        " total-iDao="+ChronoUnit.MILLIS.between(t1,t2)/1000.0+ 
//        " total-modelMap="+ChronoUnit.MILLIS.between(t3,t4)/1000.0+ 
//        " total-ShortViewService="+ChronoUnit.MILLIS.between(t4,t5)/1000.0+ 
//        " total="+ChronoUnit.MILLIS.between(t1,t6)/1000.0);
        log.debug("END :" + individualUri);
//        LogManager.getRootLogger().setLevel(Level.INFO);
        return rsv;
    }
}
