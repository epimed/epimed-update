package dao.neo4j;

import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

public class PlatformDao extends BaseDao {

	public PlatformDao() {
		super();
	}

	public PlatformDao(Transaction tx) {
		super(tx);
	}

	
	/** =============================================================================== */

	public Node findByUid(String idPlatform) {
		String query = "MATCH (p:Platform) WHERE p.uid={idPlatform} RETURN p";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idPlatform", idPlatform));
		return this.convertToNode(result, "p");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> mapProps) {
		String query = "CREATE (p:Platform) SET p = {props} RETURN p";
		StatementResult result = tx.run(query,  Values.parameters("props", mapProps));
		return this.convertToNode(result, "p");
	}

	/** =============================================================================== */

	public Node addProperties(String idPlatform, Map <String, Object> mapProps) {
		String query = "MATCH (p:Platform) WHERE p.uid={idPlatform} SET p += {props} RETURN p";
		StatementResult result = tx.run(query,  Values.parameters("idPlatform", idPlatform, "props", mapProps));
		return this.convertToNode(result, "p");
	}
	
	/** =============================================================================== */

	public Node setProperty(String idPlatform, String property, Object value) {
		String query = "MATCH (p:Platform) WHERE p.uid={idPlatform} SET p.{property}= {value} RETURN p";
		StatementResult result = tx.run(query,  Values.parameters("idPlatform", idPlatform, "propetry", property, "value", value));
		return this.convertToNode(result, "p");
	}

	/** =============================================================================== */
	
}
