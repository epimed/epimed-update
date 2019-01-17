package module.epimed_experiments;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import service.database.DatabaseTemplate;
import service.database.Mongodb;

public class DeleteOldJobs extends DatabaseTemplate {

	private Mongodb mongowww;
	private int monthsAgo = 6;

	public DeleteOldJobs() {
		mongowww = new Mongodb("config.epimed-www.mongodb.properties");
		this.addDatabase(mongowww);	
		this.execute();
	}

	@Override
	public void process()  {
		MongoDatabase db = mongowww.getMongoClient().getDatabase("epimed_experiments");
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, (-1) * monthsAgo);
		
		System.out.println(cal.getTime());
		
		Date dateMonthsAgo = cal.getTime();
		
		
		Bson filter = Filters.or (
				Filters.eq("last_download", null),
				Filters.lte("last_download", dateMonthsAgo)
				);
		
				List<Document> jobs = db.getCollection("job").find(filter).into(new ArrayList<Document>());
				System.out.println(jobs.size());
		
				for (Document job: jobs) {
					job.remove("elements");
					System.out.println(job);
					this.deleteJob(db, job.getString("_id"));
				}
	}
	
	/** ============================================================================= */
	
	private void deleteJob(MongoDatabase db, String idJob) {
		db.getCollection("job_element").deleteMany(Filters.in("job", idJob));
		db.getCollection("job").deleteOne(Filters.eq("_id", idJob));
	}
	
	/** ============================================================================= */

	public static void main(String[] args) {
		new DeleteOldJobs();

	}


}
