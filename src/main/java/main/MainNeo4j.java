package main;

import service.database.DatabaseTemplate;
import service.database.Neo4j;

public class MainNeo4j extends DatabaseTemplate {
	
	private Neo4j neo4j;
	
	public MainNeo4j() {
		neo4j = new Neo4j("config.epimed-db.neo4j.properties");
		this.addDatabase(neo4j);	
		this.execute();
	}
	
	@Override
	public void process() {
		System.out.println("Processing");
		System.out.println(neo4j.getSession());
	}

	public static void main(String[] args) {
		new MainNeo4j();
	}


}
