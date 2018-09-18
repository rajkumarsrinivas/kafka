package it.cits.ebfoundation.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;


@AllArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("squid:S1118")
public class PropertyUtil {

    private static Logger log = Logger.getLogger(PropertyUtil.class);

    static Properties props = null;

    private static final String CLASSPATH = "classpath://";

    private static boolean loadProperties(final String filename) {
        boolean theReturnValue = false;
        props = new Properties();
        for (final String path : Arrays.asList(filename, "src/main/resources/" + filename, CLASSPATH + filename)) {
            try (InputStream is = (path.startsWith(CLASSPATH) ? Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(path.replace(CLASSPATH, "")) : new FileInputStream(path))) {
                log.debug("Loading properties from {}" + path);
                if(!theReturnValue) {
                    theReturnValue = loadFile(is);
                }
            } catch (final IOException ioe) {
                log.debug("Failed to load properties from " + path, ioe);
            }
        }
        return theReturnValue;
    }

    static boolean loadFile(final InputStream theIs) {
        boolean theReturnValue = false;
        try {
            props.load(theIs);
            theReturnValue = true;
        } catch (final IOException e) {
            log.error("Failed load properties from inputstream " +  e);
        }
        return theReturnValue;
    }
    /**
     * To return the value for the given If the system environment variable is null then return value from the
     * properties
     *
     * @param
     *
     */
    public static String getProperty(final String propertyName) {
        String propertyValue = System.getenv(propertyName);
        if (propertyValue == null) {
            if (props == null) {
                loadProperties("cits.properties");
            }
            propertyValue = props.getProperty(propertyName);
        }
        return propertyValue;
    }
}
