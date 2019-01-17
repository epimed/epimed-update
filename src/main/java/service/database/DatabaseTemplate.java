package service.database;

import java.util.ArrayList;
import java.util.List;

public abstract class DatabaseTemplate {

	private List<IDatabase> databases = new ArrayList<IDatabase>();

	public final void execute() {
		System.out.println("===== START " + this.getClass().getSimpleName() + " =====");

		try  {
			this.initialize();
			this.process();
			
		}
		catch (Exception e) {
			e.printStackTrace();	
		}
		finally {
			this.terminate();
		}
		
		System.out.println("===== END " + this.getClass().getSimpleName() + " =====");		
	}
	
	
	private void initialize() {
		for (IDatabase database : databases) {
			database.connect();
			System.out.println("Connected to " + database.getConfiguration());
		}
	}
	
	private void terminate() {
		for (IDatabase database : databases) {
			database.disconnect();
			System.out.println("Disconnected from " + database.getConfiguration());
		}
	}
	
	public void addDatabase(IDatabase database) {
		databases.add(database);
	}
	
	public void removeDatabase(IDatabase database) {
		databases.remove(database);
	}
	
	public List<IDatabase> getDatabases() {
		return databases;
	}

	public abstract void process();
	
}
