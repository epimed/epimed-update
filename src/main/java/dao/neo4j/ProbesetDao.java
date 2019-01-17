package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class ProbesetDao extends BaseDao {

	public ProbesetDao() {
		super();
	}

	public ProbesetDao(Transaction tx) {
		super(tx);
	}

	/** =============================================================================== */

	public List<Node> findProbesetsWithNoRelationToNucleotides(Integer limit) {

		String query = "MATCH (p:Probeset) WHERE (NOT p.id_nucleotides=[]) AND (NOT (p)-[:LINKS]->(:Nucleotide)) RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query);
		return this.convertToList(result, "p");

	}

	/** =============================================================================== */

	public List<Node> findProbesetsWithGeneSymbolsByPlatforms(String [] idPlatforms, Integer limit) {

		String query = "MATCH (p:Probeset)-[:BELONGS_TO]->(pl:Platform) "
				+ "WHERE pl.uid IN {idPlatforms} "
				+ "AND (NOT p.gene_symbols=[]) "
				+ "AND ((p.id_genes IS NULL) OR (p.id_genes=[])) "
				+ "RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idPlatforms", idPlatforms));
		return this.convertToList(result, "p");
	}

	/** =============================================================================== */

	public List<Node> findProbesetsWithIdGenesButNoRelationToGenes(String [] idPlatforms, Integer limit) {

		String query = "MATCH (p:Probeset)-[:BELONGS_TO]->(pl:Platform) "
				+ "WHERE pl.uid IN {idPlatforms} "
				+ "AND (p.id_genes IS NOT NULL) "
				+ "AND (NOT p.id_genes=[]) "
				+ "AND (NOT (p)-[:LINKS]->(:Gene)) "
				+ "RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idPlatforms", idPlatforms));
		return this.convertToList(result, "p");
	}


	/** =============================================================================== */

	public List<Node> findProbesetsWithIdNucleotidesButNoRelationToNucleotides(String [] idPlatforms, Integer limit) {

		String query = "MATCH (p:Probeset)-[:BELONGS_TO]->(pl:Platform) "
				+ "WHERE pl.uid IN {idPlatforms} "
				+ "AND (p.id_nucleotides IS NOT NULL) "
				+ "AND (NOT p.id_nucleotides=[]) "
				+ "AND (NOT (p)-[:LINKS]->(:Nucleotide)) "
				+ "RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idPlatforms", idPlatforms));
		return this.convertToList(result, "p");
	}



	/** =============================================================================== */

	public List<Node> findProbesetsWithIdGeneAndNoRelationToGenes(String idPlatform, Integer limit) {

		String query = "MATCH (p:Probeset)-[:BELONGS_TO]->(pl:Platform) WHERE pl.uid={idPlatform} AND (NOT p.id_genes=[]) AND (NOT (p)-[:LINKS]->(:Gene)) RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idPlatform", idPlatform));
		return this.convertToList(result, "p");

	}


	/** =============================================================================== */

	public List<Node> findProbesetsWithNoRelationToGenes(String idPlatform, Integer limit) {

		String query = "MATCH (p:Probeset)-[:BELONGS_TO]->(pl:Platform) WHERE pl.uid={idPlatform} AND (NOT p.gene_symbols=[]) AND (NOT (p)-[:LINKS]->(:Gene)) RETURN p";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idPlatform", idPlatform));
		return this.convertToList(result, "p");

	}

	/** =============================================================================== */

	public Node findByUid(String idProbeset) {
		String query = "MATCH (p:Probeset) WHERE p.uid={idProbeset} RETURN p";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idProbeset", idProbeset));
		return this.convertToNode(result, "p");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> mapProps) {
		String query = "CREATE (p:Probeset) SET p = {props} RETURN p";
		StatementResult result = tx.run(query,  Values.parameters("props", mapProps));
		return this.convertToNode(result, "p");
	}

	/** =============================================================================== */

	public Node addProperties(String idProbeset, Map <String, Object> mapProps) {
		String query = "MATCH (p:Probeset) WHERE p.uid={idProbeset} SET p += {props} RETURN p";
		StatementResult result = tx.run(query,  Values.parameters("idProbeset", idProbeset, "props", mapProps));
		return this.convertToNode(result, "p");
	}

	/** =============================================================================== */

	public Relationship createRelationshipToGene(String idProbeset, Integer idGene) {

		String query = "MATCH (p:Probeset), (g:Gene) WHERE p.uid={idProbeset} AND g.uid={idGene} MERGE (p)-[r:LINKS]->(g) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idProbeset", idProbeset, "idGene", idGene));
		return this.convertToRelationship(result, "r");

	}

	/** =============================================================================== */

	public Relationship createRelationshipToNucleotide(String idProbeset, String idNucleotide) {

		String query = "MATCH (p:Probeset), (n:Nucleotide) WHERE p.uid={idProbeset} AND n.uid={idNucleotide} MERGE (p)-[r:LINKS]->(n) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idProbeset", idProbeset, "idNucleotide", idNucleotide));
		return this.convertToRelationship(result, "r");

	}
	
	/** =============================================================================== */

	public Relationship createRelationshipToPlatform(String idProbeset, String idPlatform) {

		String query = "MATCH (p:Probeset), (pl:Platform) WHERE p.uid={idProbeset} AND pl.uid={idPlatform} MERGE (p)-[r:BELONGS_TO]->(pl) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idProbeset", idProbeset, "idPlatform", idPlatform));
		return this.convertToRelationship(result, "r");

	}
	
	/** =============================================================================== */
}
