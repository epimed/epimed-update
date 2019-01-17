package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import entity.DataSource;

public class NucleotideDao extends BaseDao {

	public NucleotideDao() {
		super();
	}


	public NucleotideDao(Transaction tx) {
		super(tx);
	}

	/** =============================================================================== */

	public List<Node> findBySource(DataSource source, Integer limit) {
		String query = "MATCH (n:Nucleotide) WHERE n.source={sourcename} RETURN n";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("sourcename", source.toString()));
		return this.convertToList(result, "n");
	}


	/** =============================================================================== */

	public Node findByUid(String idNucleotide) {
		String query = "MATCH (n:Nucleotide) WHERE n.uid={idNucleotide} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idNucleotide", idNucleotide));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> mapNucleotide) {
		String query = "CREATE (n:Nucleotide) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", mapNucleotide));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idNucleotide, Map <String, Object> mapProps) {
		String query = "MATCH (n:Nucleotide) WHERE n.uid={idNucleotide} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idNucleotide", idNucleotide, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationship(String idNucleotide, int idGene) {
		String query = "MATCH (n:Nucleotide), (g:Gene) WHERE n.uid={idNucleotide} AND g.uid={idGene} MERGE (n)-[r:LINKS]->(g) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idNucleotide", idNucleotide, "idGene", idGene));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */

	public Relationship createRelationshipToPosition(String idNucleotide, String idPosition) {

		String query = "MATCH (n:Nucleotide), (p:Position) WHERE n.uid={idNucleotide} AND p.uid={idPosition} MERGE (n)-[r:LINKS]->(p) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idPosition", idPosition, "idNucleotide", idNucleotide));
		return this.convertToRelationship(result, "r");

	}
	
	/** =============================================================================== */

}
