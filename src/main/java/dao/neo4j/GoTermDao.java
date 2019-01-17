package dao.neo4j;

import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class GoTermDao extends BaseDao {

	public GoTermDao() {
		super();
	}


	public GoTermDao(Transaction tx) {
		super(tx);
	}


	/** =============================================================================== */

	public Node findByUid(String idTerm) {
		String query = "MATCH (n:GoTerm) WHERE n.uid={idTerm} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idTerm", idTerm));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idTerm, Map <String, Object> mapProps) {
		String query = "MATCH (n:GoTerm) WHERE n.uid={idTerm} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idTerm", idTerm, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node updateProperties(String idTerm, Map <String, Object> mapProps) {
		String query = "MATCH (n:GoTerm) WHERE n.uid={idTerm} SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idTerm", idTerm, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> props) {
		String query = "CREATE (n:GoTerm) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", props));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationship(int idGene, String idTerm) {
		String query = "MATCH (g:Gene), (a:GoTerm) WHERE g.uid={idGene} AND a.uid={idTerm} MERGE (g)-[r:BELONGS_TO]->(a) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene, "idTerm", idTerm));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */
	
}
