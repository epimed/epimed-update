package service.util;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;


public class FormatService {

	
	/** ================================================================================ */
	
	public List<Double> convertToListOfDouble(double[] values) {
		List<Double> result = new ArrayList<Double>();
		for (double value: values) {
			result.add(round(value, 6));
		}
		return result;
	}
	
	/** ================================================================================ */
	
	public Double pseudoLog(Double value) {
		return Math.log10(1 + value) / Math.log10(2.0);
	}
	
	/** ================================================================================ */

	public Integer asInt(Object value) {

		if (value==null ) {
			return null;
		}

		Integer result = null;
		try {
			result = (Integer) value;
		}
		
		catch (ClassCastException | NumberFormatException e1) {
		
			try {
				result = ((Long) value).intValue();
			}
			
			catch (ClassCastException | NumberFormatException e2) {
				result = Integer.parseInt((String) value);
			}
		}
		return result; 

	}

	/** ================================================================================ */

	public Double asDouble(Object value) {

		if (value==null ) {
			return null;
		}

		Double result = null;
		try {
			result = (Double) value;
		}
		catch (Exception e1) {
			result = ((Integer) value).doubleValue();
		}
		return result; 

	}

	/** ================================================================================ */

	public static double round(double value, int decimals) {
		double precision = Math.pow(10, decimals);
		return (double) Math.round(value * precision) / precision;
	}


	/** ================================================================================= */

	public Integer convertStringToInteger (String text) {
		try {
			int entryInt = Integer.parseInt(text);
			return entryInt;
		}
		catch(Exception ex) {
			return null;
		}
	}

	/** ================================================================================= */

	public List<Boolean> convertStringArrayToBooleanList (String [] stringArray) {

		List<Boolean> result = new ArrayList<Boolean> ();

		if (stringArray!=null && stringArray.length>0) {
			for (String entryString : stringArray) {

				try {

					if(entryString.toLowerCase().equals("true")) {
						result.add(true);
					}

					if(entryString.toLowerCase().equals("false")) {
						result.add(false);
					}

				}
				catch(Exception ex) {
					// nothing to do
				}
			}
		}

		if (result.isEmpty()) {
			return null;
		}

		return result;
	}

	/** ================================================================================= */

	public List<Integer> convertStringArrayToIntegerList (String [] stringArray) {

		List<Integer> integerList = new ArrayList<Integer> ();

		if (stringArray!=null && stringArray.length>0) {
			for (String entryString : stringArray) {

				try {
					int entryInt = Integer.parseInt(entryString);
					integerList.add(entryInt);
				}
				catch(Exception ex) {
					// nothing to do
				}
			}
		}

		if (integerList.isEmpty()) {
			return null;
		}

		return integerList;
	}

	/** ================================================================================= */

	public String [] convertStringToArray (String list) {
		String [] array = null;

		if (list!=null && !list.isEmpty()) {
			array = list.trim().replaceAll("['\"]", "").split("[,;\\|\\/\\p{Space}][\\p{Space}]*");
		}

		return array;
	}

	/** ================================================================================= */

	public List<Object> convertHomogeneousMongoDocuments (List<Document> listDocuments) {

		List<Object> data = new ArrayList<Object>();
		List<String> header = new ArrayList<String>();

		try  {

			// ===== Extract header =====
			header.addAll(listDocuments.get(0).keySet());

			// ===== Extract data =====
			for (Document doc : listDocuments) {
				Object [] dataLine = new Object [header.size()];

				for (int j=0; j<header.size(); j++) {
					dataLine[j] = (Object) doc.get(header.get(j));
				}
				data.add(dataLine);
			}
		}
		catch (Exception e) {
			System.out.println("ERROR in " + this.getClass().getName() + ": convertHomogeneousMongoDocuments");
		}

		return data;

	}

	/** ================================================================================= */

	public List<Object> convertHeterogeneousMongoDocuments (List<Document> listDocuments) {

		List<Object> data = new ArrayList<Object>();
		Set<String> headerSet = new HashSet<String>();
		List<String> header = new ArrayList<String>();

		try  {

			// ===== Extract header =====
			for (Document doc : listDocuments) {
				headerSet.addAll(doc.keySet());
			}
			header.addAll(headerSet);
			Collections.sort(header);


			// ===== Extract data =====
			for (Document doc : listDocuments) {
				Object [] dataLine = new Object [header.size()];

				for (int j=0; j<header.size(); j++) {
					dataLine[j] = (Object) doc.get(header.get(j));
				}
				data.add(dataLine);
			}
		}
		catch (Exception e) {
			System.out.println("ERROR in " + this.getClass().getName() + ": convertHeterogeneousMongoDocuments");
		}

		return data;

	}

	/** ================================================================================= */

	public List<String> extractHeader (List<Document> listDocuments, String rootDocumentName) {


		List<String> header = new ArrayList<String>();

		try  {


			// ===== Extract header =====

			for (Document doc : listDocuments) {

				if (rootDocumentName!=null) {
					doc = (Document) doc.get(rootDocumentName);
				}

				for (String key : doc.keySet()) {
					if (!header.contains(key)) {
						header.add(key);
					}
				}
			}
		}
		catch (Exception e) {
			System.out.println("ERROR in " + this.getClass().getName() + ": extractHeader");
		}

		return header;
	}

	/** ================================================================================= */

	public List<Object> extractData (List<Document> listDocuments, List<String> header, String rootName) {


		List<Object> data = new ArrayList<Object>();

		try  {


			// ===== Extract data =====
			for (Document doc : listDocuments) {

				if (rootName!=null) {
					doc = (Document) doc.get(rootName);
				}

				Object [] dataLine = new Object [header.size()];

				for (int j=0; j<header.size(); j++) {
					dataLine[j] = (Object) doc.get(header.get(j));
				}
				data.add(dataLine);
			}
		}
		catch (Exception e) {
			System.out.println("ERROR in " + this.getClass().getName() + ": extractData");
			System.out.println("RootName: " + rootName);
			System.out.println("Header: " + header);
			e.printStackTrace();
		}

		return data;

	}

	/** ================================================================================= */

	public static String flattenToAscii(String string) {
		char[] out = new char[string.length()];
		string = Normalizer.normalize(string, Normalizer.Form.NFD);
		int j = 0;
		for (int i = 0, n = string.length(); i < n; ++i) {
			char c = string.charAt(i);
			if (c <= '\u007F') out[j++] = c;
		}
		return new String(out);
	}

	/** ================================================================================= */

	public String normalize(String entry) {
		return flattenToAscii(entry).replaceAll("[\\p{Punct}\\p{Space}*]", "_").toLowerCase();
	}

	/** ================================================================================= */

}
