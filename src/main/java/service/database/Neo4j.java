package service.database;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class Neo4j implements IDatabase {

	private DatabaseConfiguration configuration;
	private Driver driver;
	private Session session;

	public Neo4j (String configurationFile) {
		this.configuration = this.loadConfiguration(configurationFile);
	}

	@Override
	public void connect() {
		if (this.session==null && configuration!=null) {
			this.driver = GraphDatabase.driver(configuration.getHost() + ":" + configuration.getPort(), 
					AuthTokens.basic(configuration.getUsername(), configuration.getPassword()));
			this.session = driver.session();
		}
	}

	@Override
	public void disconnect() {
		if (this.session!=null) {
			this.session.close();
			this.session = null;
		}
		if (this.driver!=null) {
			this.driver.close();
			this.driver = null;
		}
	}
	
	public Session getSession() {
		return session;
	}

	@Override
	public String toString() {
		return "Neo4j [configuration=" + configuration + "]";
	}

	@Override
	public DatabaseConfiguration getConfiguration() {
		return this.configuration;
	}



}
