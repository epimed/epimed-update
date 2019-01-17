package dao.neo4j;

import java.util.List;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

public class ProteinSequenceDao extends BaseDao {

	public ProteinSequenceDao() {
		super();
	}


	public ProteinSequenceDao(Transaction tx) {
		super(tx);
	}

	/** =============================================================================== */

	public List<Node> findByIdProtein(String idProtein) {
		String query = "MATCH (n:ProteinSequence)-[:LINKS]->(p:Protein) WHERE p.uid={idProtein} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idProtein", idProtein));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */
	
	public List<Node> findByIdGene(Integer idGene) {
		String query = "MATCH (n:ProteinSequence)-[:LINKS]->(p:Protein)-[:LINKS]->(g:Gene) WHERE g.uid={idGene} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene));
		return this.convertToList(result, "n");
	}

	
	/** =============================================================================== */
}
