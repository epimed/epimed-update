package service.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import entity.GeneStatus;

public class NcbiService {

	private WebService webService = new WebService();
	private FormatService formatService= new FormatService();

	/** =============================================================================== */

	public Document eSummary(String database, String id) {

		String url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=" + database + "&id="+ id + "&retmode=json";
		System.out.println(url);
		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse(json);
				Document summary = doc.get("result", Document.class).get(id, Document.class);
				return summary;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching an ID " + id + " on NCBI");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/** =============================================================================== */

	@SuppressWarnings("unchecked")
	public List<String> eSearch (String database, String term, Integer retmax) {

		String url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=" + database + "&term="+ term + "&retmode=json";
		System.out.println(url);
		if (retmax!=null) {
			url = url + "&retmax=" + retmax;
		}

		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse(json);
				List<String>  listIdGene = doc.get("esearchresult", Document.class).get("idlist", ArrayList.class);
				return listIdGene;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching a term " + term + " on NCBI");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/** =============================================================================== */

	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param symbol: gene_symbol or id_nucleotide
	 * @param type: GENE for gene_symbol, ACCN for id_nucleotide, UGEN for unigene
	 * @return
	 */
	public List<String> findListIdGene (String symbol, String type, int taxid) {
		try {
			String urlnuc = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gene&term="+ symbol + "[" + type + "]+AND+" + taxid + "[taxid]&retmode=json";
			System.out.println(urlnuc);
			String jsonnuc = webService.loadUrl(urlnuc);
			Document docnuc = Document.parse(jsonnuc);
			List<String>  listIdGene = docnuc.get("esearchresult", Document.class).get("idlist", ArrayList.class);
			return listIdGene;
		}
		catch (Exception e) {
			System.out.println("Exception while searching the symbol " + symbol + " from NCBI");
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return new ArrayList<String>();
		}
	}

	/** =============================================================================== */

	public Map<String, Object> findGeneAsMap (String idGene) {

		String urlgene = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gene&id=" + idGene + "&retmode=json";
		String jsongene = webService.loadUrl(urlgene);

		Document docgenefull = Document.parse(jsongene);
		Document docgene = docgenefull.get("result", Document.class).get(idGene.toString(), Document.class);

		Map<String, Object> mapGene = new HashMap<String, Object> ();

		mapGene.put("uid", Integer.parseInt(idGene));
		mapGene.put("gene_symbol", docgene.getString("name"));
		mapGene.put("chrom", docgene.getString("chromosome"));
		mapGene.put("location", docgene.getString("maplocation"));
		mapGene.put("title", docgene.getString("description"));

		// === Status ===

		Integer status = null;
		try {
			status = docgene.getInteger("status");
		}
		catch (ClassCastException e) {
			// nothing to do
		}
		if (status==null) {
			mapGene.put("status", GeneStatus.active.toString());
		}
		else {
			if (status.equals(1)) {
				mapGene.put("status", GeneStatus.replaced.toString());

				Integer currentid = null;
				try {
					currentid = docgene.getInteger("currentid");
				}
				catch (ClassCastException e) {
					// nothing to do
				}
				if (currentid!=null) {
					mapGene.put("replaced_by", currentid);
				}
			}
			if (status.equals(2)) {
				mapGene.put("status", GeneStatus.removed.toString());
			}
		}

		// === Aliases ===
		String otheraliases = docgene.getString("otheraliases");
		if (otheraliases!=null && !otheraliases.isEmpty()) {
			String [] arrayAliases = formatService.convertStringToArray(otheraliases);
			mapGene.put("aliases", arrayAliases);
		}

		// === Organism ===
		Document docorganism = docgene.get("organism", Document.class);
		Integer taxid=null;
		try {
			taxid=docorganism.getInteger("taxid");
		}
		catch (Exception e) {
			// nothing to do
		}
		if (taxid!=null) {
			mapGene.put("tax_id", taxid);
		}

		return mapGene;
	}

	/** ====================================================================================== */

	public List<String> loadGeo(String geoAccession) {

		String url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + geoAccession + "&targ=self&view=brief&form=text";

		String text = webService.loadUrl(url);
		String [] parts = text.split(System.getProperty( "line.separator" ));
		List<String> data = new ArrayList<String>(Arrays.asList(parts));

		return data;	
	}

	/** ====================================================================================== */
}
