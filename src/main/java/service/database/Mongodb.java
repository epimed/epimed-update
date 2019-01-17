package service.database;

import com.mongodb.MongoClient;

public class Mongodb implements IDatabase {

	private DatabaseConfiguration configuration;
	private  MongoClient mongoClient;

	public Mongodb (String configurationFile) {
		this.configuration = this.loadConfiguration(configurationFile);
	}

	@Override
	public void connect() {
		if (this.mongoClient==null && configuration!=null) {
			mongoClient = new MongoClient(configuration.getHost(), Integer.parseInt(configuration.getPort()));
		}
	}

	@Override
	public void disconnect() {
		if (mongoClient!=null) {
			mongoClient.close();
			mongoClient = null;
		}
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}
	
	@Override
	public String toString() {
		return "Mongodb [configuration=" + configuration + "]";
	}

	@Override
	public DatabaseConfiguration getConfiguration() {
		return this.configuration;
	}

}
