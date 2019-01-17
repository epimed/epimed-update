package main;

import service.database.DatabaseTemplate;
import service.database.Mongodb;

public class MainMongodb extends DatabaseTemplate {

	private Mongodb mongodb;

	public MainMongodb() {
		mongodb = new Mongodb("config.epimed-db.mongodb.properties");
		this.addDatabase(mongodb);	
		this.execute();
	}

	@Override
	public void process()  {
		System.out.println(mongodb.getMongoClient().getDatabase("prj_probreast"));
	}

	public static void main(String[] args) {
		new MainMongodb();

	}


}
