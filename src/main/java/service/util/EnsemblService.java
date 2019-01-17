package service.util;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

public class EnsemblService {

	private WebService webService = new WebService();

	/** ================================================================================================================== */

	public Document overlap (String feature, String species, String chrom, String start, String end, String strand) {

		String url = "https://rest.ensembl.org/overlap/region/"+species+"/"+chrom+":"+start+".."+end+":"+strand+"?feature="+feature+";content-type=application/json";

		// String url = "https://rest.ensembl.org/overlap/region/"+species+"/"+chrom+":"+start+".."+end+":"+strand+"?feature=gene;feature=transcript;feature=cds;feature=exon;content-type=application/json";
		System.out.println(url);
		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse("{result: " + json + "}");
				return doc;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching an overlap on Ensembl");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	
	/** ================================================================================================================== */

	public Document overlap (String idEnsembl, String feature) {

		String url = "http://rest.ensembl.org/overlap/id/" + idEnsembl + "?feature=" + feature + ";content-type=application/json";
		System.out.println(url);
		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse("{result: " + json + "}");
				return doc;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching an overlap on Ensembl");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/** ================================================================================================================== */

	public Document mapping (String fromAssembly, String toAssembly, String species, String chrom, String start, String end, String strand) {

		String url = "https://rest.ensembl.org/map/"+species+"/"+fromAssembly+"/"+chrom+":"+start+".."+end+":"+strand+"/"+toAssembly+"?content-type=application/json";
		System.out.println(url);
		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse(json);
				return doc;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching an overlap on Ensembl");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/** ================================================================================================================== */

	public Document lookup (String idEnsembl) {

		String url = "http://rest.ensembl.org/lookup/id/" + idEnsembl + "?content-type=application/json";
		// System.out.println(url);
		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse(json);
				return doc;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching an overlap on Ensembl");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/** ================================================================================================================== */

	public Document lookup (String idEnsembl, int expand) {

		String url = "http://rest.ensembl.org/lookup/id/" + idEnsembl + "?expand=" + expand +  "&content-type=application/json";
		System.out.println(url);
		try {
			String json = webService.loadUrl(url);
			if (json!=null && !json.isEmpty()) {
				Document doc = Document.parse(json);
				return doc;
			}
			return null;
		}
		catch (Exception e) {
			System.out.println("Exception while searching an overlap on Ensembl");
			System.out.println("URL " + url);
			System.out.println("ERROR:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/** ================================================================================================================== */

	@SuppressWarnings("unchecked")
	public List<Document> xrefsSymbol (String symbol, String species, String objectType) {

		List<Document> result = new ArrayList<Document>();

		String urlnuc = "https://rest.ensembl.org/xrefs/symbol/" + species + "/" + symbol + "?content-type=application/json";

		if (objectType!=null && !objectType.isEmpty()) {
			urlnuc = urlnuc + ";object_type=" + objectType;	
		}
		
		System.out.println(urlnuc);

		try {
			String jsonnuc = webService.loadUrl(urlnuc);

			if (jsonnuc!=null && !jsonnuc.isEmpty() && jsonnuc.startsWith("[") && jsonnuc.endsWith("]") && jsonnuc.length()>3) {
				jsonnuc = "{root: " + jsonnuc + "}";
				Document docnuc = Document.parse(jsonnuc);
				result = (List<Document>) docnuc.get("root");
			}

			return result;

		}
		catch (Exception e) {
			System.out.println("ERROR:" + e.getMessage());
			System.out.println("URL " + urlnuc);
			e.printStackTrace();
			return null;
		}

	}

	/** ======================================================================================================================== */

	@SuppressWarnings("unchecked")
	public List<Document> xrefsId (String idEnsembl, String dbname) {

		List<Document> result = new ArrayList<Document>();
		String urlnuc = "https://rest.ensembl.org/xrefs/id/" + idEnsembl + "?all_levels=1&content-type=application/json";
		if (dbname!=null && !dbname.isEmpty()) {
			urlnuc = "https://rest.ensembl.org/xrefs/id/" + idEnsembl + "?all_levels=1&external_db=" + dbname + "&content-type=application/json";
			// urlnuc = urlnuc + ";external_db=" + dbname;
		}
		System.out.println(urlnuc);	

		try {
				
			String jsonnuc = webService.loadUrl(urlnuc);

			if (jsonnuc!=null && !jsonnuc.isEmpty() && jsonnuc.startsWith("[") && jsonnuc.endsWith("]") && jsonnuc.length()>3) {

				jsonnuc = "{root: " + jsonnuc + "}";
				Document docnuc = Document.parse(jsonnuc);

				result = (List<Document>) docnuc.get("root");
			}

			return result;

		}
		catch (Exception e) {
			System.out.println("ERROR:" + e.getMessage());
			System.out.println("URL " + urlnuc);
			e.printStackTrace();
			return null;
		}

	}

	/** ======================================================================================================================== */

}
