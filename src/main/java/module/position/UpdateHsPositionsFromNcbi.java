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
import service.util.NcbiService;

public class UpdateHsPositionsFromNcbi extends DatabaseTemplate {

	private final static String SPECIES = "homo_sapiens";
	private final static Integer TAXID = 9606;

	private Mongodb mongodb;
	private Neo4j neo4j;
	private GeneDao geneDao = new GeneDao();
	private PositionDao positionDao = new PositionDao();
	private NcbiService ncbiService = new NcbiService();
	private EnsemblService ensemblService = new EnsemblService();
	private FormatService formatService = new FormatService();

	private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	private Date today = new Date();



	public UpdateHsPositionsFromNcbi() {
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
		positionDao.setTx(tx);

		List<Node> genes = geneDao.findNotLinkedToPositions(TAXID, 100000);
		// List<Node> genes = geneDao.findByTaxid(TAXID, 100000);

		// List<Node> genes = new ArrayList<Node>();
		// Node g1= geneDao.findByUid(105379117);
		// genes.add(g1);


		for (int i=0; i<genes.size(); i++) {

			Node gene = genes.get(i);
			Integer idGene = gene.get("uid").asInt();
			String geneSymbol = gene.get("gene_symbol").asString();

			System.out.println("---------------------------------------------------------");
			System.out.println((i+1) + "/" + genes.size() + " Processing gene " + geneSymbol + " [" + idGene + "] " 
					+ gene.get("type").asString() + " " + gene.get("status").asString());

			Document geneNcbi = ncbiService.eSummary("gene", idGene.toString());

			if (geneNcbi!=null) {

				geneNcbi.append("_id", idGene);

				List<Document> transcripts = this.extractTranscripts(geneNcbi);

				for (Document transcript: transcripts) {
					System.out.println(transcript);
					Map <String, Object> position = this.generatePositionNcbi(transcript, gene);
					System.out.println(position);
					Node nodePosition = this.saveOrUpdate(position);
					System.out.println(nodePosition.asMap());

				}
			}

			// mongodb.getMongoClient().getDatabase("ensembl").getCollection("summary").insertOne(geneSummary);

		}
		tx.success();
		// tx.close();
	}


	/** ============================================================================= */

	@SuppressWarnings("unchecked")
	private List<Document> extractTranscripts(Document geneSummary) {
		List<Document> transcripts = new ArrayList<Document>();
		transcripts.addAll(geneSummary.get("genomicinfo", ArrayList.class));
		return transcripts;
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

	private Map <String, Object> generatePositionNcbi(Document transcript, Node gene) {

		Set<Object> assemblies = new HashSet<Object>();
		assemblies.add("GRCh38");

		Map <String, Object> position = new LinkedHashMap<String, Object> ();

		String idTranscript = transcript.getString("chraccver").split("\\.")[0];

		position.put("uid", idTranscript);
		position.put("id_position", transcript.getString("chraccver"));
		position.put("tax_id", gene.get("tax_id").asInt());
		position.put("id_assemblies", assemblies);
		position.put("id_gene", gene.get("uid").asInt());
		position.put("chrom", "chr" + gene.get("chrom").asString());

		Integer start = transcript.getInteger("chrstart");
		Integer end = transcript.getInteger("chrstop");
		String strand = null;

		if (start <= end) {
			strand = "+";
		}
		else {
			strand = "-";
		}

		position.put("strand", strand);

		position.put("tx_start", Math.min(start, end));
		position.put("tx_end", Math.max(start, end));

		position.put("id_ensembl", null);
		position.put("canonical", true);
		position.put("source", "ncbi");

		try {
			position.put("exon_count", formatService.asInt(transcript.get("exoncount")));
		}
		catch (ClassCastException | NumberFormatException e) {
			position.put("exon_count", null);
			e.printStackTrace();
		}

		position.put("exon_starts", null);
		position.put("exon_ends", null);
		position.put("last_update", dateFormat.format(today));

		return position;
	}


	/** ============================================================================= */

	public static void main(String[] args) {
		new UpdateHsPositionsFromNcbi();

	}


}
