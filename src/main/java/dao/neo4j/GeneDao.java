package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class GeneDao extends BaseDao {

	public GeneDao() {
		super();
	}


	public GeneDao(Transaction tx) {
		super(tx);
	}
	
	/** =============================================================================== */

	public Node findCurrentByUid(Integer uid) {
		String query = "MATCH (g1:Gene)-[*0..1]->(g2:Gene) WHERE g1.uid={uid} AND NOT g2.status=\"replaced\" RETURN g2";
		StatementResult result = tx.run(query, Values.parameters("uid", uid));
		return this.convertToNode(result, "g2");
	}

	/** =============================================================================== */

	public List<Node> findAllWithLocation() {
		String query = "MATCH (g:Gene) WHERE EXISTS(g.location) AND g.location IS NOT NULL RETURN g";
		StatementResult result = tx.run(query);
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByStatus(String status, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.status={status} RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("status", status));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findByStatusWithoutIdEnsembl(String status, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.status={status} AND g.id_ensembl IS NULL RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("status", status));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByTaxidAndStatusDifferentFrom(int taxid, String status, Integer limit) {
		String query = "MATCH (g:Gene) WHERE  g.tax_id={taxid} AND (g.status IS NULL OR NOT g.status={status}) RETURN g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid, "status", status));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByTaxid(int taxid, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findActiveByTaxid(int taxid, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND g.status=\"active\" RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid));
		return this.convertToList(result, "g");
	}
	
	
	/** =============================================================================== */

	public List<Node> findByTaxidAndTitleContains(int taxid, String wordInTitle, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND LOWER(g.title) CONTAINS LOWER({wordInTitle}) AND NOT LOWER(g.title) CONTAINS \"pseudogene\" RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid, "wordInTitle", wordInTitle));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findByTaxidWithIdEnsembl(int taxid, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND g.id_ensembl IS NOT NULL RETURN g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findByTaxidAndSymbolContains(int taxid, String symbol, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND g.gene_symbol CONTAINS {symbol} RETURN g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid, "symbol", symbol));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */
	
	public List<Node> findByChromAndTaxid(String chrom, int taxid, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.chrom={chrom} AND g.tax_id={taxid} RETURN g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("chrom", chrom, "taxid", taxid));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByStatusAndTaxid(String status, int taxid, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.status={status} AND g.tax_id={taxid} RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("status", status, "taxid", taxid));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findByStatusAndTaxidAndType(String status, int taxid, String type, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.status={status} AND g.tax_id={taxid} AND g.type= {type} RETURN g ORDER BY g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("status", status, "taxid", taxid, "type", type));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public Node findByUid(Integer idGene) {
		String query = "MATCH (g:Gene) WHERE g.uid={idGene} RETURN g";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByGeneSymbol(String geneSymbol, int taxid) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND g.gene_symbol={geneSymbol} RETURN g";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("geneSymbol", geneSymbol, "taxid", taxid));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public Node findActiveByGeneSymbol(String geneSymbol, int taxid) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND g.gene_symbol={geneSymbol} AND g.status=\"active\" RETURN g";
		StatementResult result = tx.run(query, Values.parameters("geneSymbol", geneSymbol, "taxid", taxid));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public Node findActiveByIdReplacedGene(int idReplacedGene) {
		String query = "MATCH (gr:Gene)-[r:REPLACED_BY]->(g:Gene) WHERE gr.uid= {idReplacedGene} RETURN g";
		StatementResult result = tx.run(query, Values.parameters("idReplacedGene", idReplacedGene));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByAlias(String geneSymbol, int taxid) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND {geneSymbol} in g.aliases RETURN g";
		StatementResult result = tx.run(query, Values.parameters("geneSymbol", geneSymbol, "taxid", taxid));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public Node findActiveByAlias(String geneSymbol, int taxid) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND {geneSymbol} in g.aliases AND g.status=\"active\" RETURN g";
		StatementResult result = tx.run(query, Values.parameters("geneSymbol", geneSymbol, "taxid", taxid));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByGeneSymbolOrAlias(String geneSymbol, int taxid) {
		String query = "MATCH (g:Gene) WHERE g.tax_id={taxid} AND (g.gene_symbol={geneSymbol} OR {geneSymbol} in g.aliases) RETURN distinct g";
		StatementResult result = tx.run(query, Values.parameters("geneSymbol", geneSymbol, "taxid", taxid));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByAnnotation(String idAnnotation, Integer limit) {
		String query = "MATCH (g:Gene)-[r:BELONGS_TO]->(a:Annotation) WHERE a.uid= {idAnnotation} RETURN g ORDER BY g.uid ASC";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idAnnotation", idAnnotation));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findByTaxidAndAnnotationType(String type, int taxid, Integer limit) {
		String query = "MATCH (g:Gene)-[r:BELONGS_TO]->(a:Annotation) WHERE a.type={type} AND g.tax_id={taxid} RETURN DISTINCT g ORDER BY g.uid ASC";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("type", type, "taxid", taxid));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public List<Node> findByAnnotationAndTaxid(String idAnnotation, int taxid, Integer limit) {
		String query = "MATCH (g:Gene)-[r:BELONGS_TO]->(a:Annotation) WHERE g.tax_id={taxid} AND a.uid={idAnnotation} RETURN g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idAnnotation", idAnnotation, "taxid", taxid));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Node> findCurrentByAnnotationAndTaxid(String idAnnotation, int taxid, Integer limit) {
		String query = "MATCH (g1:Gene)-[*0..1]->(g:Gene)-[r:BELONGS_TO]->(a:Annotation) WHERE g.tax_id={taxid} AND a.uid={idAnnotation} AND NOT g.status=\"replaced\" RETURN distinct g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idAnnotation", idAnnotation, "taxid", taxid));
		return this.convertToList(result, "g");
	}
	
	/** =============================================================================== */

	public List<Integer> listUidCurrentByAnnotationAndTaxid(String idAnnotation, int taxid, Integer limit) {
		String query = "MATCH (g1:Gene)-[*0..1]->(g:Gene)-[r:BELONGS_TO]->(a:Annotation) WHERE g.tax_id={taxid} AND a.uid={idAnnotation} AND NOT g.status=\"replaced\" RETURN distinct g.uid";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("idAnnotation", idAnnotation, "taxid", taxid));
		return this.convertToListInteger(result, "g.uid");
	}

	/** =============================================================================== */

	public Node findByUidAndAnnotation(int idGene, String idAnnotation) {
		String query = "MATCH (g:Gene)-[r:BELONGS_TO]->(a:Annotation) WHERE g.uid= {idGene} AND a.uid= {idAnnotation} RETURN g";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene, "idAnnotation", idAnnotation));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public Node findByProbeset(String idProbeset) {
		String query = "MATCH (p:Probeset)-[r:LINKS]->(g:Gene) WHERE p.uid={idProbeset}  RETURN g";
		StatementResult result = tx.run(query, Values.parameters("idProbeset", idProbeset));
		return this.convertToNode(result, "g");
	}


	/** =============================================================================== */

	public List<Node> findNotLinkedToPositions(int taxid, Integer limit) {
		String query = "MATCH (g:Gene) WHERE g.tax_id= {taxid} AND NOT (:Position)-[:LINKS]->(g) AND NOT g.aliases=[] RETURN g";
		if (limit!=null) {
			query = query + " LIMIT " + limit;
		}
		StatementResult result = tx.run(query, Values.parameters("taxid", taxid));
		return this.convertToList(result, "g");
	}

	/** =============================================================================== */

	public Node addProperties(int idGene, Map <String, Object> mapProps) {
		String query = "MATCH (g:Gene) WHERE g.uid={idGene} SET g += {props} RETURN g";
		StatementResult result = tx.run(query,  Values.parameters("idGene", idGene, "props", mapProps));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> mapGene) {
		String query = "CREATE (g:Gene) SET g = {props} RETURN g";
		StatementResult result = tx.run(query,  Values.parameters("props", mapGene));
		return this.convertToNode(result, "g");
	}

	/** =============================================================================== */

	public Relationship createRelationship(int idReplacedGene, int idActiveGene) {
		String query = "MATCH (gr:Gene), (ga:Gene) WHERE gr.uid={idReplacedGene} AND ga.uid={idActiveGene} MERGE (gr)-[r:REPLACED_BY]->(ga) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idReplacedGene", idReplacedGene, "idActiveGene", idActiveGene));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */
}
