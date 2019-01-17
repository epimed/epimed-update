package service.database;

import java.util.Properties;

public interface IDatabase {
	
	public void connect();
	public void disconnect();
	public DatabaseConfiguration getConfiguration();
	
	public default DatabaseConfiguration loadConfiguration(String configurationFile) {
		ConfigurationLoader loader = ConfigurationLoader.getInstance();
		Properties properties = loader.load(configurationFile);
		DatabaseConfiguration configurationDatabase = new DatabaseConfiguration (
				properties.getProperty("host"),
				properties.getProperty("port"),
				properties.getProperty("username"),
				properties.getProperty("password")
				);
		return configurationDatabase;
	}

}
