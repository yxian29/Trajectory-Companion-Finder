package common.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;

public class PropertyFileParser {
    private static final Logger LOG = Logger.getLogger(PropertyFileParser.class.getName());

    private Properties props = new Properties();
    private String propFileName;

    public PropertyFileParser(String propFileName) {
        this.propFileName = propFileName;
    }

    public String getPropFileName() {
        return this.propFileName;
    }

    public void setPropFileName(String filename) {
        this.propFileName = filename;
    }

    public String getProperty(String key) {
        return props.get(key).toString();
    }

    public void parseFile() throws Exception {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        try {
            if(inputStream != null) {
                props.load(inputStream);
            } else {
                throw new IOException("failed to load property file");
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
            ex.printStackTrace();
            try {
                File file = new File(propFileName);
                inputStream = new FileInputStream(file.getAbsolutePath());
                props.load(inputStream);
            } catch (IOException ex1) {
                LOG.log(Level.SEVERE, "failed to load property file");
                ex1.printStackTrace();
                throw ex1;
            }
        }
    }
}
