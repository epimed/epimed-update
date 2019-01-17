package module.gene;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;

import dao.neo4j.GeneDao;
import entity.GeneStatus;
import service.database.DatabaseTemplate;
import service.database.Neo4j;
import service.util.WebService;

public class UpdateGeneStatus extends DatabaseTemplate {


	private Neo4j neo4j;
	private GeneDao geneDao = new GeneDao();
	private WebService webService = new WebService();



	public UpdateGeneStatus() {
		neo4j = new Neo4j("config.epimed-db.neo4j.properties");
		this.addDatabase(neo4j);	
		this.execute();
	}

	@Override
	public void process()  {

		Transaction tx = neo4j.getSession().beginTransaction();
		geneDao.setTx(tx);

		List<Node> genes = geneDao.findByStatus("unknown", 100000);
		
		/*
		List<Node> genesHs = geneDao.findByTaxid(9606, 100000);
		List<Node> genesMm = geneDao.findByTaxid(10090, 100000);
		List<Node> genes = new ArrayList<Node>();
		genes.addAll(genesHs);
		genes.addAll(genesMm);
		 */
		
		for (int i=0; i<genes.size(); i++) {

			// ===== Begin transaction =====
			if (tx!=null) {
				if (tx.isOpen()) {
					tx.close();
				}
				tx = neo4j.getSession().beginTransaction();
			}
			geneDao.setTx(tx);

			System.out.println("-------------------------------------");

			Node gene = genes.get(i);
			int idGene = gene.get("uid").asInt();
			String geneSymbol = gene.get("gene_symbol").asString();
			int taxid = gene.get("tax_id").asInt();

			System.out.println((i+1) + "/" + genes.size() + "\t" + idGene + "\t" + geneSymbol + "\t(taxid=" + taxid + ")");
			System.out.println(gene.asMap());

			Map<String,Object> mapProps = this.createGeneFromNcbi(idGene);
			Node updatedGene = geneDao.addProperties(idGene, mapProps);
			System.out.println("Gene " + geneSymbol + " updated from status " + gene.get("status").toString()
					+ " to status " + updatedGene.get("status").toString());


			if (updatedGene.get("status").asString().equals("replaced")) {
				int idActiveGene = updatedGene.get("replaced_by").asInt();
				Node activeGene = geneDao.findByUid(idActiveGene);
				if (activeGene==null) {
					Map<String,Object> mapActiveProps = this.createGeneFromNcbi(idActiveGene);
					activeGene = geneDao.addProperties(idActiveGene, mapActiveProps);
					System.out.println("Current active gene " + activeGene.get("uid").asInt() 
							+ " " + activeGene.get("gene_symbol").asString() + " was missing, now created");
				}
				geneDao.createRelationship(idGene, idActiveGene);
				System.out.println("Relationship created: " + updatedGene.get("uid").asInt() 
						+ " " + updatedGene.get("gene_symbol").asString()
						+ " -> " + + activeGene.get("uid").asInt() 
						+ " " + activeGene.get("gene_symbol").asString());
			}

			// tx.close();
			tx.success();
		}
	}


	/** ============================================================================= */

	public Map<String, Object> createGeneFromNcbi (Integer idGene) {

		Map<String, Object> mapProps = new HashMap<String, Object>();


		String urlgene = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gene&id=" + idGene + "&retmode=json";
		System.out.println(urlgene);

		String jsongene = webService.loadUrl(urlgene);

		if (jsongene!=null && !jsongene.isEmpty()) {

			Document docgenefull = Document.parse(jsongene);
			Document docgene = docgenefull.get("result", Document.class).get(idGene.toString(), Document.class);


			// === Status ===

			Object status = docgene.get("status");
			if (status==null || status.equals("")) {
				mapProps.put("status", GeneStatus.active.name());
			}
			else {
				if (status.equals("1") || status.equals(1)) {
					mapProps.put("status", GeneStatus.replaced.name());
					Object currentid = docgene.get("currentid");
					if (currentid!=null) {
						Integer intCurrentid = null;
						try {
							intCurrentid = (Integer) currentid;
						}
						catch (ClassCastException e1) {
							try {
								intCurrentid = Integer.parseInt( (String) currentid);
							}
							catch (ClassCastException e2) {
								e2.printStackTrace();
							}
						}
						if (intCurrentid!=null) {
							mapProps.put("replaced_by", intCurrentid);
						}
					}
				}
				if (status.equals("2") || status.equals(2)) {
					mapProps.put("status", GeneStatus.removed.name());
				}
			}
		}

		return mapProps;
	}

	/** ============================================================================= */

	public static void main(String[] args) {
		new UpdateGeneStatus();

	}

	/** ============================================================================= */

}
