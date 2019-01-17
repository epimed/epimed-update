package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class GoGroupDao extends BaseDao {

	public GoGroupDao() {
		super();
	}


	public GoGroupDao(Transaction tx) {
		super(tx);
	}


	/** =============================================================================== */

	public Node findByUid(String idGroup) {
		String query = "MATCH (n:GoGroup) WHERE n.uid={idGroup} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idGroup", idGroup));
		return this.convertToNode(result, "n");
	}
	
	/** =============================================================================== */

	public Node findByName(String name) {
		String query = "MATCH (n:GoGroup) WHERE n.name={name} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("name", name));
		return this.convertToNode(result, "n");
	}
	
	/** =============================================================================== */

	public List<Node> findByAspect(String aspect) {
		String query = "MATCH (n:GoGroup) WHERE n.aspect={aspect} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("aspect", aspect));
		return this.convertToList(result, "n");
	}
	
	/** =============================================================================== */
	
	public List<Node> findByIdGeneOrderByPriority(int idGene) {
		String query = "MATCH (g:Gene)-[]->(t:GoTerm)-[]->(n:GoGroup) WHERE g.uid={idGene} RETURN DISTINCT n ORDER BY n.priority";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idGroup, Map <String, Object> mapProps) {
		String query = "MATCH (n:GoGroup) WHERE n.uid={idGroup} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idGroup", idGroup, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node updateProperties(String idGroup, Map <String, Object> mapProps) {
		String query = "MATCH (n:GoGroup) WHERE n.uid={idGroup} SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idGroup", idGroup, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> props) {
		String query = "CREATE (n:GoGroup) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", props));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationship(String idTerm, String idGroup) {
		String query = "MATCH (g:GoTerm), (a:GoGroup) WHERE g.uid={idTerm} AND a.uid={idGroup} MERGE (g)-[r:BELONGS_TO]->(a) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idTerm", idTerm, "idGroup", idGroup));
		return this.convertToRelationship(result, "r");
	}
	
	/** =============================================================================== */
}
