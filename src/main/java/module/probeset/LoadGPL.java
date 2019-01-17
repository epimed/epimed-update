package module.probeset;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;

import dao.neo4j.GeneDao;
import dao.neo4j.NucleotideDao;
import dao.neo4j.PlatformDao;
import dao.neo4j.ProbesetDao;
import entity.DataSource;
import service.database.DatabaseTemplate;
import service.database.Neo4j;
import service.util.FileService;
import service.util.NcbiService;

public class LoadGPL extends DatabaseTemplate {


	private Neo4j neo4j;
	private boolean COMMIT = true;
	private int batchSize = 100;

	private String workingDir = System.getProperty("user.dir");
	private FileService fileService = new FileService();
	private NcbiService ncbiService = new NcbiService();

	// ===== DAO =====
	private GeneDao geneDao = new GeneDao();
	private PlatformDao platformDao = new PlatformDao();
	private ProbesetDao probesetDao = new ProbesetDao();
	private NucleotideDao nucleotideDao = new NucleotideDao();


	public LoadGPL() {
		neo4j = new Neo4j("config.epimed-db.neo4j.properties");
		this.addDatabase(neo4j);	
		this.execute();
	}

	@Override
	public void process() {
		System.out.println("Processing");
		System.out.println(neo4j.getSession());

		Transaction tx = neo4j.getSession().beginTransaction(); 
		this.openTransaction(tx);

		// ====== Load file ======

		String inputfile = "GPL6244.txt";
		String filename =  workingDir + File.separator + "data" + File.separator + "in" + File.separator + inputfile;
		System.out.println("LOADING \t " + filename);
		List<String[]> csv = fileService.loadCsv(filename, '\t');
		fileService.printHeader(csv,3);

		String idPlatform = "GPL6244";
		Integer taxid = 9606;

		Node pl = this.getPlatform(idPlatform, taxid);
		System.out.println(pl.asMap());

		Map<String, Object> mapProps = new HashMap<String,Object>();
		Set<String> idGenes = new HashSet<String>();
		Set<String> idNucleotides = new HashSet<String>();


		Set<String> geneSymbols = new HashSet<String>();

		Set<String> idPlatforms = new HashSet<String>();

		// for (int i=1; i<csv.size(); i++) {
		for (int i=16500; i<csv.size(); i++) {

			if (tx==null || !tx.isOpen()) {
				this.openTransaction(tx);
			}

			String [] line = csv.get(i);

			System.out.println("----- " + i);
			// System.out.println("----- " + Arrays.toString(line));

			mapProps.clear();
			idGenes.clear();
			idNucleotides.clear();
			geneSymbols.clear();

			// idGenes = this.recognizeString(line, new int [] {8});
			// System.out.println(idGenes);

			geneSymbols = this.recognizeString(line, new int [] {9});
			// System.out.println(geneSymbols);

			// idNucleotides = this.recognizeString(line, new int [] {1});
			// System.out.println(idNucleotides);


			String idProbeset = line[0].trim();
			Node p = probesetDao.findByUid(idProbeset);

			mapProps.put("uid", idProbeset);
			mapProps.put("source", DataSource.probeset.name());
			mapProps.put("id_genes", idGenes);
			mapProps.put("gene_symbols", geneSymbols);
			mapProps.put("id_nucleotides", idNucleotides);

			idPlatforms.clear();
			idPlatforms.add(idPlatform);

			if (p==null) {	
				mapProps.put("id_platforms", idPlatforms);
				p = probesetDao.save(mapProps);
			}
			else {
				List<Object> listExistingPlatforms = p.get("id_platforms").asList();
				for (Object existingPlatform : listExistingPlatforms) {
					idPlatforms.add((String) existingPlatform);
				}
				mapProps.put("id_platforms", idPlatforms);
				probesetDao.addProperties(idProbeset, mapProps);
			}

			System.out.println(i + " Probeset " + p.asMap());

			// ===== Relationaships ===

			// Gene
			for (String idGene : idGenes) {
				this.createRelationshipToGene(idProbeset, idGene);
			}

			// Gene Symbols
			if (idGenes.isEmpty() && !geneSymbols.isEmpty()) {
				for (String geneSymbol : geneSymbols) {
					this.createRelationshipToGeneSymbol(idProbeset, geneSymbol, taxid);
				}
			}

			// Nucleotides
			for (String idNucleotide : idNucleotides) {
				this.createRelationshipToNucleotide(idProbeset, idNucleotide);
				if (idGenes.isEmpty()) {
					this.createRelationshipNucleotideToGene(idNucleotide, taxid);
				}
			}


			// Platform
			probesetDao.createRelationshipToPlatform(idProbeset, idPlatform);

			if (i%batchSize==0) {
				this.closeTransaction(tx);
				System.out.println("--- Transaction finished ---");
			}

		}

		this.closeTransaction(tx);


	}

	/** ============================================================================================== */

	private void createRelationshipNucleotideToGene(String idNucleotide, int taxid) {

		List<String> ncbiIdGenes = ncbiService.findListIdGene(idNucleotide, "ACCN", taxid);
		if (ncbiIdGenes.isEmpty() && (idNucleotide.toLowerCase().startsWith("hs") || idNucleotide.toLowerCase().startsWith("mm"))) {
			ncbiIdGenes = ncbiService.findListIdGene(idNucleotide, "UGEN", taxid);
		}

		for (String ncbiIdGene : ncbiIdGenes) {
			Node gene = geneDao.findCurrentByUid(Integer.parseInt(ncbiIdGene));
			if (gene!=null) {
				nucleotideDao.createRelationship(idNucleotide, gene.get("uid").asInt());
				System.out.println("\t Gene from ACCN NCBI " + gene.get("uid").asInt() + " " + gene.get("gene_symbol").asString());
			}
		}	
	}

	/** ============================================================================================== */

	private void createRelationshipToNucleotide(String idProbeset, String idNucleotide) {

		if (idNucleotide!=null && !idNucleotide.isEmpty()) {

			Map<String, Object> mapNuc = new HashMap<String, Object>();

			Node n = nucleotideDao.findByUid(idNucleotide);
			if (n==null) {
				mapNuc.clear();
				mapNuc.put("uid", idNucleotide);
				if (idNucleotide.contains("_")) {
					mapNuc.put("source", DataSource.refsec.name());
				}
				else if (idNucleotide.startsWith("Mm") || idNucleotide.startsWith("Hs")) {
					mapNuc.put("source", DataSource.unigene.name());
				}
				else {
					mapNuc.put("source", DataSource.genbank.name());
				}
				n = nucleotideDao.save(mapNuc);
			}

			probesetDao.createRelationshipToNucleotide(idProbeset, idNucleotide);
		}

	}

	/** ============================================================================================== */

	private void createRelationshipToGene(String idProbeset, String idGene) {

		Integer uidGene = Integer.parseInt(idGene);

		Node gene = geneDao.findByUid(uidGene);

		if (gene==null) {
			Map<String, Object> geneMap = ncbiService.findGeneAsMap(idGene);
			gene = geneDao.save(geneMap);
		}

		// System.out.println("\t Gene from UID " + gene.get("uid").asInt() + " " + gene.get("gene_symbol").asString());

		probesetDao.createRelationshipToGene(idProbeset, uidGene);
	}

	/** ============================================================================================== */

	private void createRelationshipToGeneSymbol(String idProbeset, String geneSymbol, int taxid) {

		Node gene =  geneDao.findActiveByGeneSymbol(geneSymbol, taxid);

		if (gene==null) {
			List<String> ncbiIdGenes = ncbiService.findListIdGene(geneSymbol, "GENE", taxid);

			if (ncbiIdGenes!=null && ncbiIdGenes.size()>0) {
				for (String ncbiIdGene : ncbiIdGenes) {
					gene = geneDao.findCurrentByUid(Integer.parseInt(ncbiIdGene));
					if (gene!=null) {
						probesetDao.createRelationshipToGene(idProbeset, gene.get("uid").asInt());
						System.out.println("\t Gene from GENE NCBI " + gene.get("uid").asInt() + " " + gene.get("gene_symbol").asString());
					}
				}	
			}
		}

		else {
			probesetDao.createRelationshipToGene(idProbeset, gene.get("uid").asInt());
			System.out.println("\t Gene from GENE SYMBOL " + gene.get("uid").asInt() + " " + gene.get("gene_symbol").asString());
		}



	}

	/** ============================================================================================== */

	private Set<String> recognizeString(String [] line, int [] indexes) {

		Set<String> set = new HashSet<String>();
		for (int index: indexes) {
			if (index<line.length) {
				String item = line[index];
				if (item!=null && !item.isEmpty()) {
					String [] symbols = item.split("[///;//,]");
					for (String symbol: symbols) {
						symbol = symbol.trim();
						if (symbol.contains("_")) {
							symbol = symbol.split("\\.")[0];
						}
						if (symbol!=null && !symbol.isEmpty() && !symbol.contains("---") && !symbol.trim().contains(" ")) {
							set.add(symbol.trim());
						}
					}
				}
			}
		}
		return set;
	}


	/** ============================================================================================== */

	private Node getPlatform (String idPlatform, Integer taxid) {
		Node pl = platformDao.findByUid(idPlatform);

		if (pl!=null) {
			Map<String, Object> mapPlatform = new HashMap<String,Object>();
			mapPlatform.put("enabled", true);
			pl = platformDao.addProperties(idPlatform, mapPlatform);
		}

		/*
		if (pl==null) {
			Map<String, Object> mapPlatform = new HashMap<String,Object>();
			mapPlatform.put("uid", idPlatform);
			mapPlatform.put("title", "[Mouse430_2] Affymetrix Mouse Genome 430 2.0 Array");
			mapPlatform.put("manufacturer", "	Affymetrix");
			mapPlatform.put("tax_id", taxid);
			mapPlatform.put("technology", "in situ oligonucleotide");
			mapPlatform.put("type", "expression array");
			mapPlatform.put("enabled", true);
			pl = platformDao.save(mapPlatform);
		}
		 */

		return pl;
	}

	/** ============================================================================================== */

	private void openTransaction(Transaction tx) {
		platformDao.setTx(tx);
		probesetDao.setTx(tx);
		nucleotideDao.setTx(tx);
		geneDao.setTx(tx);
	}

	/** ============================================================================================== */

	private void closeTransaction(Transaction tx) {
		if (COMMIT) {
			tx.success();
		}
		else {
			tx.close();
		}
	}

	/** ============================================================================================== */

	public static void main(String[] args) {
		new LoadGPL();
	}

}
