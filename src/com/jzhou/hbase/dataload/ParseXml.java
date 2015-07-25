package com.jzhou.hbase.dataload;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.ByteArrayInputStream;
import java.util.*;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParseXml {

	//private static Logger LOG = LoggerFactory.getLogger(ParseXml.class);
	private static final Log LOG = LogFactory.getLog(ParseXml.class);
	
    public HashMap<String, String> getXmlTags(String line) {
         
    	
        HashMap<String, String> fieldMap = new HashMap<String, String>();
        String   dataTag = "data";
        String  monthTag = "month";
        
        StringBuilder month = new StringBuilder();   
        StringBuilder data = new StringBuilder();
        
        try {
                         
            //XMLInputFactory factory = XMLInputFactory.newFactory();
             
            //XMLStreamReader rawReader = factory.createXMLStreamReader(new ByteArrayInputStream(line.getBytes()));
             
             
            //XMLStreamReader filteredReader = factory.createFilteredReader(rawReader,
            //          new StreamFilter() {
            //            public boolean accept(XMLStreamReader r) {
            //              return !r.isWhiteSpace();
            //            }
            //         });
        	XMLStreamReader filteredReader = XMLInputFactory.newInstance()
        			.createXMLStreamReader(new ByteArrayInputStream(line.getBytes()));
        	
            String currentElement = "";
                      
            while (filteredReader.hasNext()) {
                System.out.println("reader");
                int code = filteredReader.next();
                 
                switch (code) {
                 
                    case XMLStreamReader.START_ELEMENT:
                        currentElement = filteredReader.getLocalName();
                        System.out.println("current element "+ currentElement);
                        break;
                    
                    case XMLStreamReader.CHARACTERS:
                        
                    	if (currentElement.equalsIgnoreCase(monthTag)) {
                            month.append(filteredReader.getText().trim());
                        }
                    	else if (currentElement.equalsIgnoreCase(dataTag)) {
                            data.append(filteredReader.getText().trim());
                        }
                        break;
                }
                
            }
        } catch (XMLStreamException e) {
            e.printStackTrace();
        } catch (FactoryConfigurationError e) {
            e.printStackTrace();
        }
        
        fieldMap.put(month.toString(), data.toString());
        
        return fieldMap;  
    }

   
}
