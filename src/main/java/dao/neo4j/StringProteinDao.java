package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import entity.RelationshipType;

public class StringProteinDao extends BaseDao {

	public StringProteinDao() {
		super();
	}


	public StringProteinDao(Transaction tx) {
		super(tx);
	}

	/** =============================================================================== */

	public Node findByUid(String idProtein) {
		String query = "MATCH (n:StringProtein) WHERE n.uid={idProtein} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idProtein", idProtein));
		return this.convertToNode(result, "n");
	}
	
	/** =============================================================================== */

	public List<Node> findByIdGene(int idGene) {
		String query = "MATCH (n:StringProtein) WHERE n.id_gene={idGene} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByTaxid(int taxid) {
		String query = "MATCH (n:StringProtein) WHERE n.tax_id={taxid} RETURN n ORDER BY n.uid";
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public Relationship findConnection(int idGene1, int idGene2) {
		String query = "MATCH (n1:StringProtein {id_gene:{idGene1}})-[r:INTERACTS]-(n2:StringProtein {id_gene:{idGene2}}) RETURN r";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idGene1", idGene1, "idGene2", idGene2));
		return this.convertToRelationship(result, "r");
	}
	
	/** =============================================================================== */

	public  List<Path> findPaths(int idGene1, int idGene2, int maxArcs, Integer limit) {
		String query = "MATCH p=((n1:StringProtein {id_gene:{idGene1}})-[*1.." + maxArcs + "]-(n2:StringProtein {id_gene:{idGene2}})) RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idGene1", idGene1, "idGene2", idGene2));
		return this.convertToListOfPaths(result, "p");
	}
	
	/** =============================================================================== */

	public List<Path> findPaths(String geneSymbol1, String geneSymbol2, int maxArcs, int limit) {
		String query = "MATCH p=((n1:StringProtein {gene_symbol:{geneSymbol1}})-[*1.." + maxArcs + "]-(n2:StringProtein {gene_symbol:{geneSymbol2}})) RETURN p LIMIT " + limit;
		System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("geneSymbol1", geneSymbol1, "geneSymbol2", geneSymbol2));
		return this.convertToListOfPaths(result, "p");
	}

	/** =============================================================================== */

	public Path findShortestPath(String geneSymbol1, String geneSymbol2, int maxArcs) {
		String query = "MATCH p=shortestPath((n1:StringProtein {gene_symbol:{geneSymbol1}})-[*1.." + maxArcs + "]-(n2:StringProtein {gene_symbol:{geneSymbol2}})) RETURN p";
		System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("geneSymbol1", geneSymbol1, "geneSymbol2", geneSymbol2));
		return this.convertToPath(result, "p");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> map) {
		String query = "CREATE (n:StringProtein) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", map));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idProtein, Map <String, Object> mapProps) {
		String query = "MATCH (n:StringProtein) WHERE n.uid={idProtein} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idProtein", idProtein, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationship(String idProtein, int idGene) {
		String query = "MATCH (p:StringProtein), (g:Gene) WHERE p.uid={idProtein} AND g.uid={idGene} MERGE (p)-[r:LINKS]->(g) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idProtein", idProtein, "idGene", idGene));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */

	public Relationship createUniqueRelationshipInteracts(String idProtein1, String idProtein2, int experimental, int database) {

		if (experimental==0 && database==0) {
			return null;
		}

		// === Query existing relationship ===
		String queryExisting = "MATCH (p1:StringProtein)-[r:INTERACTS]-(p2:StringProtein) WHERE p1.uid={idProtein1} AND p2.uid={idProtein2} RETURN r";
		StatementResult resultExisting = tx.run(queryExisting, Values.parameters("idProtein1", idProtein1, "idProtein2", idProtein2));
		Relationship existing = this.convertToRelationship(resultExisting, "r");

		// === Skip if relationship exists already ===
		if (existing!=null) {
			return existing;
		}

		// === Create new relationship otherwise ===		
		String query = "MATCH (p1:StringProtein), (p2:StringProtein) WHERE p1.uid={idProtein1} AND p2.uid={idProtein2} "
				+ "MERGE (p1)-[r:INTERACTS" 
				+ " {"
				+ "experimental: {experimental},"
				+ "database: {database}"
				+ "}"
				+ "]->(p2) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idProtein1", idProtein1, "idProtein2", idProtein2, "experimental", experimental, "database", database));
		return this.convertToRelationship(result, "r");

	}

	/** =============================================================================== */

	public Relationship createRelationship(RelationshipType relationshipName, String idProtein1, String idProtein2, int score) {
		if (score>0) {
			String query = "MATCH (p1:StringProtein), (p2:StringProtein) WHERE p1.uid={idProtein1} AND p2.uid={idProtein2} "
					+ "MERGE (p1)-[r:" + relationshipName.name()
					+ " {"
					+ "score: {score}"
					+ "}"
					+ "]->(p2) RETURN r";
			StatementResult result = tx.run(query, Values.parameters("idProtein1", idProtein1, "idProtein2", idProtein2, "score", score));
			return this.convertToRelationship(result, "r");
		}
		else {
			return null;
		}
	}

	/** =============================================================================== */


}
