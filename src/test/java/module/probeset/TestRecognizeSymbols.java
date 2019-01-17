package module.probeset;

import java.util.HashSet;
import java.util.Set;

public class TestRecognizeSymbols {
	
	public TestRecognizeSymbols() {
		
		String [] line = new String[2];
		line [0] = "SP38; ZPBP1 /// NM_007009.1 ; ";
		line [1] = "SP38; ZPBP1";

		Set<String> set = this.recognizeString(line, new int [] {0, 1});
		System.out.println(set);
		
	}
	
	public static void main (String[] args) {
		 new TestRecognizeSymbols();
	}

	
	/** ============================================================================================== */

	private Set<String> recognizeString(String [] line, int [] indexes) {
		
		Set<String> set = new HashSet<String>();
		for (int index: indexes) {
			String item = line[index];
			if (item!=null && !item.isEmpty()) {
				String [] symbols = item.split("[///;]");
				for (String symbol: symbols) {
					symbol = symbol.trim();
					if (symbol.contains("_")) {
						symbol = symbol.split("\\.")[0];
					}
					if (symbol!=null && !symbol.isEmpty() && !symbol.equals("---")) {
						set.add(symbol);
					}
				}
			}
		}
		return set;
	}
	/** ============================================================================================== */
}
