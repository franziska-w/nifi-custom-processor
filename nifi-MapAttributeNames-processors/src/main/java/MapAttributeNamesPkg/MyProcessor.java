/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package MapAttributeNamesPkg;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
//import org.apache.nifi.processor.util.StandardValidators;

import CSVToMapServicePkg.MyService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"map","attribute","name","mapping"})
@CapabilityDescription("Replaces attribute names accroding to a given mapping.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {
	
	//public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
    //        .Builder().name("MY_PROPERTY")
    //        .displayName("My property")
    //        .description("Example Property")
    //        .required(true)
    //        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    //        .build();
    
    public static final PropertyDescriptor MAP_SERVICE = new PropertyDescriptor
    		.Builder().name("CSVToMapService")
    		.displayName("CSVToMapService")
    		.description("Reads mapping from a CSV file.")
    		.required(true)
    		.identifiesControllerService(MyService.class)
    		.build();    

    //public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
    //        .name("MY_RELATIONSHIP")
    //        .description("Example relationship")
    //        .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sucessfully mapped attribute names")
            .build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to map attribute names")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        //descriptors.add(MY_PROPERTY);
        descriptors.add(MAP_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        //relationships.add(MY_RELATIONSHIP);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        // TODO implement
        
        //get attributes
    	Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
    	Set<String> attributesToDelete = new HashSet<>();
    	
    	//get mapping
    	final MyService mappingService = context.getProperty(MAP_SERVICE).asControllerService(MyService.class);
    	Map<String, String> mapping = mappingService.getMapping();
    	
    	try {
        	//loop through mapping
        	for (Map.Entry<String, String> mapping_entry : mapping.entrySet()) {
        		
        		//check if mapping attribute name input is in attributes
        		if (attributes.containsKey(mapping_entry.getKey())) {
        			
        			//add attribute name input with attribute name output from mapping
        			attributes.put(mapping_entry.getValue(), attributes.get(mapping_entry.getKey()));
        			
        			//set attribute with attribute name input for deletion
        			attributesToDelete.add(mapping_entry.getKey());
        		}
        	}

        	//set output
            flowFile = session.putAllAttributes(flowFile, attributes);
            flowFile = session.removeAllAttributes(flowFile, attributesToDelete);
            
            //return success output
            session.transfer(flowFile, REL_SUCCESS);
            
        } catch (Exception e) {
        	
        	//send file to failure output
        	getLogger().error(e.getMessage());
        	session.transfer(flowFile, REL_FAILURE);
        }
        finally {}
    }
}
