package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class ProteinDao extends BaseDao {

	public ProteinDao() {
		super();
	}


	public ProteinDao(Transaction tx) {
		super(tx);
	}

	/** =============================================================================== */

	public Node findByUid(String idProtein) {
		String query = "MATCH (n:Protein) WHERE n.uid={idProtein} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idProtein", idProtein));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByIdGene(Integer idGene) {
		String query = "MATCH (p:Protein)-[r:LINKS]->(g:Gene) WHERE g.uid={idGene} RETURN p";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene));
		return this.convertToList(result, "p");
	}
	
	/** =============================================================================== */

	public Node save(Map <String, Object> mapProtein) {
		String query = "CREATE (n:Protein) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", mapProtein));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idProtein, Map <String, Object> mapProps) {
		String query = "MATCH (n:Protein) WHERE n.uid={idProtein} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idProtein", idProtein, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationship(String idProtein, int idGene) {
		String query = "MATCH (n:Protein), (g:Gene) WHERE n.uid={idProtein} AND g.uid={idGene} MERGE (n)-[r:LINKS]->(g) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idProtein", idProtein, "idGene", idGene));
		return this.convertToRelationship(result, "r");
	}
	
	/** =============================================================================== */
}
