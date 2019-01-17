package module.position;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;

import dao.neo4j.GeneDao;
import dao.neo4j.PositionDao;
import service.database.DatabaseTemplate;
import service.database.Mongodb;
import service.database.Neo4j;
import service.util.EnsemblService;
import service.util.FormatService;

public class UpdateHsPositionsFromEnsembl extends DatabaseTemplate {

	private final static String SPECIES = "homo_sapiens";
	private final static Integer TAXID = 9606;

	private Mongodb mongodb;
	private Neo4j neo4j;
	private GeneDao geneDao = new GeneDao();
	private PositionDao positionDao = new PositionDao();
	private EnsemblService ensemblService = new EnsemblService();
	private FormatService formatService = new FormatService();

	private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	private Date today = new Date();



	public UpdateHsPositionsFromEnsembl() {
		neo4j = new Neo4j("config.epimed-db.neo4j.properties");
		this.addDatabase(neo4j);	
		mongodb = new Mongodb("config.epimed-db.mongodb.properties");
		this.addDatabase(mongodb);	
		this.execute();
	}

	@Override
	public void process()  {

		Transaction tx = neo4j.getSession().beginTransaction();
		geneDao.setTx(tx);
		geneDao.setTx(tx);
		positionDao.setTx(tx);

		// List<Node> genes = geneDao.findNotLinkedToPositions(TAXID, 100000);
		List<Node> genes = geneDao.findByTaxid(TAXID, 100000);

		/*
		List<Node> genes = new ArrayList<Node>();
		Node g1= geneDao.findByUid(1);
		genes.add(g1);
		*/

		for (int i=0; i<genes.size(); i++) {

			Node gene = genes.get(i);
			Integer idGene = gene.get("uid").asInt();
			String geneSymbol = gene.get("gene_symbol").asString();

			System.out.println("---------------------------------------------------------");
			System.out.println((i+1) + "/" + genes.size() + " Processing gene " + geneSymbol + " [" + idGene + "] " 
					+ gene.get("type").asString() + " " + gene.get("status").asString());


			List<String> listIdEnsembl  = this.extractListIdEnsembl(geneSymbol, SPECIES);

			for (String idEnsembl: listIdEnsembl) {

				Document lookup = ensemblService.lookup(idEnsembl, 1);


				if (lookup!=null) {
					List<Document> transcripts = this.extractTranscripts(idGene, lookup);
					for (Document transcript: transcripts) {
						Map <String, Object> position = this.generatePosition(transcript, gene);
						Node nodePosition = this.saveOrUpdate(position);
						System.out.println(nodePosition.asMap());
					}
				}
			}
		}

		tx.success();
	}

	/** ============================================================================= */

	@SuppressWarnings("unchecked")
	private Node saveOrUpdate(Map <String, Object> position) {

		Set<Object> setAssemblies = (Set<Object>) position.get("id_assemblies");

		Integer idGene = (Integer) position.get("id_gene");
		String idPosition = (String) position.get("uid");
		Node nodePosition = positionDao.findByUid(idPosition);

		if (!setAssemblies.isEmpty()) {

			if (nodePosition==null) {
				nodePosition = positionDao.save(position);
			}
			else {
				List<Object> listAssemblies = nodePosition.get("id_assemblies").asList();
				setAssemblies.addAll(listAssemblies);
				position.put("id_assemblies", setAssemblies);
				nodePosition = positionDao.addProperties(idPosition, position);
			}

			positionDao.createRelationshipToGene(idPosition, idGene);

			List<Object> listAssemblies = nodePosition.get("id_assemblies").asList();
			for (Object assembly: listAssemblies) {
				String idAssembly = (String) assembly;
				positionDao.createRelationshipToAssembly(idPosition, idAssembly);
			}

		}
		return nodePosition;
	}

	/** ============================================================================= */

	@SuppressWarnings("unchecked")
	private Map <String, Object> generatePosition(Document transcript, Node gene) {

		Set<Object> assemblies = new HashSet<Object>();
		assemblies.add(transcript.getString("assembly_name"));

		Map <String, Object> position = new LinkedHashMap<String, Object> ();
		position.put("uid", transcript.getString("id"));
		position.put("id_position", transcript.getString("id"));
		position.put("tax_id", gene.get("tax_id").asInt());
		position.put("id_assemblies", assemblies);
		position.put("id_gene", gene.get("uid").asInt());
		position.put("chrom", "chr" + gene.get("chrom").asString());
		
		String strand = null;
		if (transcript.getInteger("strand")!=null) {
			if (transcript.getInteger("strand").equals(1)) {
				strand = "+";
			}
			if (transcript.getInteger("strand").equals(-1)) {
				strand = "-";
			}
		}

		position.put("strand", strand);
		position.put("tx_start", transcript.getInteger("start"));
		position.put("tx_end", transcript.getInteger("end"));

		position.put("id_ensembl", transcript.getString("id"));
		position.put("canonical", transcript.getInteger("is_canonical").equals(1) ? true : false);
		position.put("source", "ensembl");

		List<Document> exons = transcript.get("Exon", ArrayList.class);
		List<Integer> exonStarts = new ArrayList<Integer>();
		List<Integer> exonEnds = new ArrayList<Integer>();

		for (Document exon: exons) {
			exonStarts.add(exon.getInteger("start"));
			exonEnds.add(exon.getInteger("end"));
		}
		position.put("exon_count", exons.size());
		position.put("exon_starts", exonStarts);
		position.put("exon_ends", exonEnds);
		position.put("last_update", dateFormat.format(today));

		return position;
	}


	/** ============================================================================= */

	@SuppressWarnings("unchecked")
	private List<Document> extractTranscripts(Integer idGene, Document lookup) {

		List<Document> transcripts = new ArrayList<Document>();

		String idEnsembl = lookup.getString("id"); 
		List<Document> xrefs = ensemblService.xrefsId(idEnsembl, "EntrezGene");
		List<Document> xrefs2 = ensemblService.xrefsId(idEnsembl, "WikiGene");
		xrefs.addAll(xrefs2);

		int i=0; boolean isFound = false;
		while (!isFound && i<xrefs.size()) {
			Document xref = xrefs.get(i);
			String primaryId = xref.getString("primary_id");
			Integer idGeneXref = formatService.convertStringToInteger(primaryId);
			if (idGene.equals(idGeneXref)) {
				isFound = true;
				transcripts.addAll(lookup.get("Transcript", ArrayList.class));
			}
			i++;
		}

		return transcripts;
	}

	/** ============================================================================= */

	private List<String> extractListIdEnsembl(String geneSymbol, String species) {
		List<String> listIdEnsembl = new ArrayList<String>();
		List<Document> listDocuments = ensemblService.xrefsSymbol(geneSymbol, species, "gene");
		for (Document document: listDocuments) {
			String idEnsembl = document.getString("id");
			listIdEnsembl.add(idEnsembl);
		}
		return listIdEnsembl;
	}

	/** ============================================================================= */

	public static void main(String[] args) {
		new UpdateHsPositionsFromEnsembl();

	}


}
