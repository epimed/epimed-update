package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import entity.DataSource;

public class PositionDao extends BaseDao {

	public PositionDao() {
		super();
	}


	public PositionDao(Transaction tx) {
		super(tx);
	}

	/** =============================================================================== */

	public Node findByUid(String idPosition) {
		String query = "MATCH (n:Position) WHERE n.uid={idPosition} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idPosition", idPosition));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByTaxidAndContainsSymbol(int taxid, String symbol) {
		String query = "MATCH (n:Position) WHERE n.tax_id={taxid} AND n.id_position CONTAINS {symbol} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid, "symbol", symbol));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByIdEnsembl(String idEnsembl, DataSource dataSource) {
		String query = "MATCH (n:Position) WHERE n.id_ensembl={idEnsembl} AND n.source={dataSource} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idEnsembl", idEnsembl, "dataSource", dataSource.name()));
		return this.convertToList(result, "n");
	}


	/** =============================================================================== */

	public List<Node> findByIdGeneAndIdAssembly(int idGene, String idAssembly) {
		String query = "MATCH (n:Position)-[:LINKS]->(g:Gene) WHERE g.uid={idGene} AND {idAssembly} IN n.id_assemblies RETURN n ORDER BY n.exon_count DESC";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene, "idAssembly", idAssembly));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findWithIdEnsemblNotLinkedToGene(int taxid, Integer limit) {
		String query = "MATCH (n:Position) WHERE n.tax_id= {taxid} AND n.id_ensembl IS NOT NULL AND NOT (n)-[:LINKS]->(:Gene) RETURN n";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public Node findCanonical(int idGene, String idAssembly) {
		String query = "MATCH (n:Position) WHERE n.id_gene={idGene} AND n.canonical=true AND {idAssembly} IN n.id_assemblies RETURN n ORDER BY n.source LIMIT 1";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene, "idAssembly", idAssembly));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> mapProps) {
		String query = "CREATE (n:Position) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idPosition, Map <String, Object> mapProps) {
		String query = "MATCH (n:Position) WHERE n.uid={idPosition} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idPosition", idPosition, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationshipToGene(String idPosition, int idGene) {
		String query = "MATCH (n:Position), (g:Gene) WHERE n.uid={idPosition} AND g.uid={idGene} MERGE (n)-[r:LINKS]->(g) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idPosition", idPosition, "idGene", idGene));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */

	public Relationship createRelationshipToAssembly(String idPosition, String idAssembly) {

		String query = "MATCH (p:Position), (a:Assembly) WHERE p.uid={idPosition} AND a.uid={idAssembly} MERGE (p)-[r:BELONGS_TO]->(a) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idPosition", idPosition, "idAssembly", idAssembly));
		return this.convertToRelationship(result, "r");

	}

	/** =============================================================================== */

	public Relationship createRelationshipToPosition(String uidUcsc, String uidEnsembl) {

		String query = "MATCH (p1:Position {uid:{uidUcsc}}), (p2:Position {uid:{uidEnsembl}}) WHERE MERGE (p1)-[r:LINKS]->(p1) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("uidUcsc", uidUcsc, "uidEnsembl", uidEnsembl));
		return this.convertToRelationship(result, "r");

	}

	/** =============================================================================== */

}
