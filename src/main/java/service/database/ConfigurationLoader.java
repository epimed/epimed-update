package service.database;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationLoader {

	private static final String userDir = System.getProperty("user.dir");
	private static final String fileSeparator = System.getProperty("file.separator");
	
	
	private static volatile ConfigurationLoader instance = null;
	public Properties properties = new Properties();

	private ConfigurationLoader() {
		super();
	}

	public final static ConfigurationLoader getInstance() {
		if (ConfigurationLoader.instance == null) {
			synchronized(ConfigurationLoader.class) {
				if (ConfigurationLoader.instance == null) {
					ConfigurationLoader.instance = new ConfigurationLoader();
				}
			}
		}
		return ConfigurationLoader.instance;
	}

	public Properties load(String inputfile) {
		
		

		InputStream input = null;

		try {
			input = new FileInputStream(userDir + fileSeparator + inputfile);
			properties.load(input);
		} 
		catch (IOException ex) {
			ex.printStackTrace();
		} 
		finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return properties;
	}

}
