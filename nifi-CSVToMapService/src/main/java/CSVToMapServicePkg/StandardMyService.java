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
package CSVToMapServicePkg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

//added
import java.util.Map;
import java.util.HashMap;
import java.lang.Iterable;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Tags({ "csv","mapping"})
@CapabilityDescription("ControllerService to get a Mapping from a CSV file.")
public class StandardMyService extends AbstractControllerService implements MyService {

    //public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
    //        .Builder().name("MY_PROPERTY")
    //        .displayName("My Property")
    //        .description("Example Property")
    //        .required(true)
    //        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    //        .build();

    public static final PropertyDescriptor CSV_FILE = new PropertyDescriptor
            .Builder().name("csv-file")
            .displayName("CSV File")
            .description("A CSV file, that must have a header row.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(true)
            .build();
    
    public static final PropertyDescriptor CSV_FORMAT = new PropertyDescriptor
            .Builder().name("csv-format")
            .displayName("CSV Format")
            .description("Specifies which \"format\" the CSV data is in, or specifies if custom formatting should be used.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(Arrays.asList(CSVFormat.Predefined.values()).stream().map(e -> e.toString()).collect(Collectors.toSet()))
            .required(true)
            .build();
    
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor
            .Builder().name("Character Set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to decode the CSV file.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(true)
            .build();
    
    public static final PropertyDescriptor INPUT_ATTRIBUTE_NAME_COLUMN =
            new PropertyDescriptor.Builder()
                .name("input-attribute-name-column")
                .displayName("Input Attribute Name Column")
                .description("Column containing attribute names of input data of processor.")
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(true)
                .build();

        public static final PropertyDescriptor OUTPUT_ATTRIBUTE_NAME_COLUMN =
            new PropertyDescriptor.Builder()
                .name("output-lookup-value-column")
                .displayName("Output Attribute Name Column")
                .description("Column containing attribute names for output data of processor.")
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(true)
                .build();
    
    private static final List<PropertyDescriptor> properties;
    private static final Map<String, String> mapping = new HashMap<>();

    private volatile String csvFile;
    private volatile CSVFormat csvFormat;
    private volatile String charset;
    private volatile String attributeNameColumnInput;
    private volatile String attributeNameColumnOutput;
    
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        //props.add(MY_PROPERTY);
        props.add(CSV_FILE);
        props.add(CSV_FORMAT);
        props.add(CHARSET);
        props.add(INPUT_ATTRIBUTE_NAME_COLUMN);
        props.add(OUTPUT_ATTRIBUTE_NAME_COLUMN);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     * @throws FileNotFoundException 
     */
    
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, FileNotFoundException {
    	
    	//get properties
    	this.csvFile = context.getProperty(CSV_FILE).evaluateAttributeExpressions().getValue();
        this.csvFormat = CSVFormat.Predefined.valueOf(context.getProperty(CSV_FORMAT).getValue()).getFormat();
        this.charset = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();
        this.attributeNameColumnInput = context.getProperty(INPUT_ATTRIBUTE_NAME_COLUMN).evaluateAttributeExpressions().getValue();
        this.attributeNameColumnOutput = context.getProperty(OUTPUT_ATTRIBUTE_NAME_COLUMN).evaluateAttributeExpressions().getValue();
        
        //load csv
        try (final InputStream is = new FileInputStream(csvFile)) {
        	try (final InputStreamReader reader = new InputStreamReader(is, charset)){
        		final Iterable<CSVRecord> records = csvFormat.withFirstRecordAsHeader().parse(reader);
        		
        		//write records to map
        		for (final CSVRecord record : records) {
        			final String attributeNameInput = record.get(attributeNameColumnInput);
                    final String attributeNameOutput = record.get(attributeNameColumnOutput);
                    mapping.put(attributeNameInput, attributeNameOutput);
        		}
        	}
        } catch (IOException e) {
			// TODO Auto-generated catch block
		}
    }

    @OnDisabled
    public void shutdown() {

    }

    @Override
    public void execute() throws ProcessException {

    }
    
    @Override
    public Map<String, String> getMapping(){
    	return mapping;
    }
}
