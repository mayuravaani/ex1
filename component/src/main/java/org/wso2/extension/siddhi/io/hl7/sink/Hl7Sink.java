package org.wso2.extension.siddhi.io.hl7.sink;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.Initiator;
import ca.uhn.hl7v2.llp.LLPException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.Parser;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.hl7.util.hl7Constants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.util.Map;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

@Extension(
        name = "hl7",
        namespace = "sink",
        description = "The hl7 pushes the hl7 message into HAPI client using MLLP protocol ",
        parameters = {
                @Parameter(name = "host.name ",
                        description = "The target host where the HL7 message will be pushed" ,
                        type = {DataType.STRING }),

                @Parameter(name = "port",
                        description = "This is the unique logical address used to establish the connection for the process.",
                        type = {DataType.INT}),

                @Parameter(name = "hl7.encoding",
                        description = "Encoding method of hl7",
                        optional = true, defaultValue = "ER-7",
                        type = {DataType.STRING}),

                @Parameter(name = "charset",
                        description = "Character encoding method",
                        optional = true, defaultValue = "UTF-8",
                        type ={DataType.STRING} )
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sinks

public class Hl7Sink extends Sink {
    private static final Logger log = Logger.getLogger(Hl7Sink.class);

    private String hostName;
    private int port;
    private boolean tlsEnabled;
    private String charset;
    private StreamDefinition streamDefinition;
    private HapiContext hapiContext = null;
    private Connection connection = null;
    private Message response = null;
    private String responseString = null;


    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
            return new Class[0];
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
            return new String[0];
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param streamDefinition  containing stream definition bind to the {@link Sink}
     * @param optionHolder            Option holder containing static and dynamic configuration related
     *                                to the {@link Sink}
     * @param configReader        to read the sink related system configuration.
     * @param siddhiAppContext        the context of the {@link SiddhiApp} used to
     *                                get siddhi related utility functions.
     */
    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
            SiddhiAppContext siddhiAppContext) {
            this.streamDefinition = streamDefinition;
            this.hostName = optionHolder.validateAndGetStaticValue(hl7Constants.HL7_SERVER_HOST_NAME);
            this.port = Integer.parseInt(optionHolder.validateAndGetStaticValue(hl7Constants.HL7_PORT_NO));
            this.charset = optionHolder.validateAndGetStaticValue(hl7Constants.CHARSET_NAME);
            this.tlsEnabled = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(hl7Constants.TLS_ENABLE));

    }

    /**
     * This method will be called when events need to be published via this sink
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
        Initiator initiator = connection.getInitiator();
        Parser parser = hapiContext.getPipeParser();
        try {
            Message message = parser.parse(String.valueOf(payload));
            try {
                response = initiator.sendAndReceive(message);
                responseString = parser.encode(response);
                log.debug(response);

            } catch (LLPException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }


        } catch (HL7Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        hapiContext = new DefaultHapiContext();
        try {
            connection = hapiContext.newClient(hostName,port,tlsEnabled);



        } catch (HL7Exception e) {
            //e.printStackTrace();
            log.error("Error creating the hapi client"+ e);
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {

    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
     * This is also used to identify the internal states and debugging
     * @return all internal states should be return as an map with meaning full keys
     */
    @Override
    public Map<String, Object> currentState() {
            return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     *              This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {

    }
}

