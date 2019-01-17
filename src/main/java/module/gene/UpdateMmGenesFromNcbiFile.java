package module.gene;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;

import dao.neo4j.GeneDao;
import service.database.DatabaseTemplate;
import service.database.Neo4j;
import service.util.FileService;

public class UpdateMmGenesFromNcbiFile extends DatabaseTemplate {


	private Neo4j neo4j;
	private GeneDao geneDao = new GeneDao();
	private FileService fileService = new FileService();
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	private String workingDir = System.getProperty("user.dir");
	private Date today = new Date();



	public UpdateMmGenesFromNcbiFile() {
		neo4j = new Neo4j("config.epimed-db.neo4j.properties");
		this.addDatabase(neo4j);	
		this.execute();
	}

	@Override
	public void process()  {

		Transaction tx = neo4j.getSession().beginTransaction();
		geneDao.setTx(tx);


		// ====== Load file download from NCBI ======
		String inputfile = "Mus_musculus.gene_info";
		// String filename =  workingDir + File.separator + "data" + File.separator + "in" + File.separator + inputfile;
		String filename =  workingDir + File.separator + inputfile;
		System.out.println("LOADING \t " + filename);
		List<String[]> csv = fileService.loadCsv(filename, '\t');
		fileService.printHeader(csv);
		Map<String, Integer> mapHeader = fileService.generateHeaderMap(csv);
		System.out.println(mapHeader);


		for (int i=1; i<csv.size(); i++) {
		// for (int i=1; i<20; i++) {

			String [] line = csv.get(i);
			// System.out.println(Arrays.toString(line));

			Integer idGene = Integer.parseInt(line[mapHeader.get("GeneID")]);
			Node gene = geneDao.findByUid(idGene);
			// System.out.println(gene.asMap());


			if (gene==null || gene.asMap()==null || gene.asMap().isEmpty()) {
				Map<String, Object> mapProps = this.createMapPropsFromLine(line, null, mapHeader);
				mapProps.put("status", "unknown");
				Node createdGene = geneDao.save(mapProps);
				System.out.println("Gene created \t" + createdGene.asMap());
			}
			else {
				// System.out.println("Initial gene \t" + gene.asMap());
				Map<String, Object> mapProps = this.createMapPropsFromLine(line, gene.asMap(), mapHeader);
				// System.out.println("Updated properties \t" + mapProps);
				geneDao.addProperties(idGene, mapProps);
			}

			if (i%1000==0) {
				System.out.println("----- Updated " + i + " genes -----");
			}

		}

		tx.success();
	}


	/** =========================================================================== */

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Map<String, Object> createMapPropsFromLine(String[] line, Map<String, Object> mapPropsOrig, Map<String, Integer> mapHeader) {

		Map<String, Object> mapProps = new HashMap<String, Object>();

		// === Taxid ===
		mapProps.put("tax_id", Integer.parseInt(line[mapHeader.get("#tax_id")]));

		// === Gene ID ===
		mapProps.put("uid", Integer.parseInt(line[mapHeader.get("GeneID")]));

		// === Symbol ===
		mapProps.put("gene_symbol", line[mapHeader.get("Symbol")]);

		// === Aliases ===

		Set<String> aliases = new HashSet<String>();

		if (mapPropsOrig!=null && mapPropsOrig.get("aliases")!=null) {
			aliases.addAll((Collection) mapPropsOrig.get("aliases"));
		}

		String [] synonyms = line[mapHeader.get("Synonyms")].split("\\|");
		for (String s : synonyms) {
			if (!s.trim().isEmpty() && !s.equals("-")) {
				aliases.add(s.trim());
			}
		}
		String [] dbXrefs = line[mapHeader.get("dbXrefs")].split("\\|");
		for (String x : dbXrefs) {
			String [] refs = x.split(":");
			if (refs!=null && refs.length>0) {
				String refType = refs[0];
				String ref = refs[refs.length-1];
				if (x.startsWith("MIM")) {
					ref = "MIM:"+ ref;
				}
				if (x.startsWith("HGNC")) {
					ref = "HGNC:"+ ref;
				} 

				if (!ref.trim().isEmpty() && !ref.equals("-")) {
					aliases.add(ref.trim());
				}
				
				if (refType!=null && refType.equals("Ensembl")) {
					mapProps.put("id_ensembl", ref.trim());
				}
			}
		}
		mapProps.put("aliases", aliases);

		// === Chromosome ===
		mapProps.put("chrom", line[mapHeader.get("chromosome")]);

		// === Location ===
		mapProps.put("location", line[mapHeader.get("map_location")]);

		// === Title ===
		mapProps.put("title", line[mapHeader.get("description")]);

		// === Type ===
		mapProps.put("type", line[mapHeader.get("type_of_gene")]);

		// === Feature ===
		if (line.length>mapHeader.get("Feature_type")) {
			String feature = line[mapHeader.get("Feature_type")];
			if ( feature!=null && !feature.equals("-")) {
				mapProps.put("feature", feature);
			}
		}


		// === Modification date ===
		if (line.length>mapHeader.get("Modification_date")) {
			mapProps.put("modification_date", line[mapHeader.get("Modification_date")]);
		}

		// === Last update ===
		mapProps.put("last_update", dateFormat.format(today));

		return mapProps;
	}

	/** ============================================================================= */

	public static void main(String[] args) {
		new UpdateMmGenesFromNcbiFile();

	}

	/** ============================================================================= */

}
