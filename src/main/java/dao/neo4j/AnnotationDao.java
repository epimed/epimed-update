package dao.neo4j;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

public class AnnotationDao extends BaseDao {

	public AnnotationDao() {
		super();
	}


	public AnnotationDao(Transaction tx) {
		super(tx);
	}


	/** =============================================================================== */

	public Node findByUid(String idAnnotation) {
		String query = "MATCH (n:Annotation) WHERE n.uid={idAnnotation} RETURN n";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("idAnnotation", idAnnotation));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findAll() {
		String query = "MATCH (n:Annotation) RETURN n ORDER BY n.source ASC, n.parameter ASC";
		StatementResult result = tx.run(query);
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByType(String type) {
		String query = "MATCH (n:Annotation) WHERE n.type= {type} RETURN n ORDER BY n.source ASC, n.parameter ASC";
		StatementResult result = tx.run(query, Values.parameters("type", type));
		return this.convertToList(result, "n");
	}


	/** =============================================================================== */

	public List<String> findUidByParameter(String parameter) {
		String query = "MATCH (n:Annotation) WHERE n.parameter CONTAINS {parameter} RETURN n.uid ORDER BY n.uid";
		StatementResult result = tx.run(query, Values.parameters("parameter", parameter));
		return this.convertToListString(result, "n.uid");
	}
	
	/** =============================================================================== */

	public List<String> findUidByParameters(List<String> parameters) {
		String query = "MATCH (n:Annotation) WHERE n.parameter IN {parameters} RETURN DISTINCT n.uid ORDER BY n.uid";
		StatementResult result = tx.run(query, Values.parameters("parameters", parameters));
		return this.convertToListString(result, "n.uid");
	}
	
	/** =============================================================================== */

	public List<Node> findByParameter(String parameter) {
		String query = "MATCH (n:Annotation) WHERE n.parameter={parameter} RETURN n";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("parameter", parameter));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByGene(int idGene) {
		String query = "MATCH (g:Gene)-[r:BELONGS_TO]->(n:Annotation) WHERE g.uid= {idGene} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findByGeneAndSource(int idGene, String source) {
		String query = "MATCH (g:Gene)-[r:BELONGS_TO]->(n:Annotation) WHERE g.uid= {idGene} AND  n.source={source} RETURN n";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene, "source", source));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public List<Node> findBySource(String source) {
		String query = "MATCH (n:Annotation) WHERE n.source={source} RETURN n";
		// System.out.println(query);
		StatementResult result = tx.run(query, Values.parameters("source", source));
		return this.convertToList(result, "n");
	}

	/** =============================================================================== */

	public Node addProperties(String idAnnotation, Map <String, Object> mapProps) {
		String query = "MATCH (n:Annotation) WHERE n.uid={idAnnotation} SET n += {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("idAnnotation", idAnnotation, "props", mapProps));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Node save(Map <String, Object> props) {
		String query = "CREATE (n:Annotation) SET n = {props} RETURN n";
		StatementResult result = tx.run(query,  Values.parameters("props", props));
		return this.convertToNode(result, "n");
	}

	/** =============================================================================== */

	public Relationship createRelationship(int idGene, String idAnnotation) {
		String query = "MATCH (g:Gene), (a:Annotation) WHERE g.uid={idGene} AND a.uid={idAnnotation} MERGE (g)-[r:BELONGS_TO]->(a) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idGene", idGene, "idAnnotation", idAnnotation));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */

	public Relationship createRelationship(String idNucleotide, String idAnnotation) {
		String query = "MATCH (n:Nucleotide), (a:Annotation) WHERE n.uid={idNucleotide} AND a.uid={idAnnotation} MERGE (n)-[r:BELONGS_TO]->(a) RETURN r";
		StatementResult result = tx.run(query, Values.parameters("idNucleotide", idNucleotide, "idAnnotation", idAnnotation));
		return this.convertToRelationship(result, "r");
	}

	/** =============================================================================== */
}
