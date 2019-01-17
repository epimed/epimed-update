package service.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;

public class WebService {

	private URL url;
	private boolean DEBUG = false;

	
	/** ====================================================================================== */

	public String loadUrl (String urlString) {

		String result = null;

		if (urlString!=null && !urlString.isEmpty()) {
			try {
				HttpURLConnection conn = null;

				conn = openConnection(urlString, true);

				if (conn!=null && conn.getInputStream()!=null) {
					BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

					result = readAll(br);

					br.close();
					conn.disconnect();
				}
			}

			catch (IOException e) {
				e.printStackTrace();
			} 
		}
		return result;
	}



	/** ======================================================================================*/

	private String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}


	/** ====================================================================================== */

	public HttpURLConnection openConnection(String urlString, boolean isJson) {

		int maxNbConn=5;
		boolean isOpen = false;

		int currentConnNb=0;
		int waitSeconds = 10;

		if (urlString!=null && !urlString.isEmpty()) {

			while (!isOpen && currentConnNb<maxNbConn) {
				currentConnNb ++;
				HttpURLConnection conn;
				try {	
					if (DEBUG) {System.out.println(urlString);}
					URL url = new URL(urlString);
					conn = (HttpURLConnection) url.openConnection();		

					conn.setRequestMethod("GET");
					if (isJson) {
						conn.setRequestProperty("Accept", "application/json");
					}

					if (DEBUG) {System.out.println("Response code=" + conn.getResponseCode());}

					// if (conn.getResponseCode()>=400 && conn.getResponseCode()<=407) {
					if (conn.getResponseCode()>=502) {
						// Bad request / Forbidden / Not Found etc
						return null;
					}


					isOpen = true;
					return conn;
				} 
				catch (IOException e) {
					e.printStackTrace();
					try {
						System.err.println("Lost connection to the host " + url + " (trial " +  currentConnNb + "/"+ maxNbConn + ")");
						System.err.println("New trial in " + waitSeconds + " sec");
						isOpen = false;
						Thread.sleep(waitSeconds*1000);
					}
					catch (InterruptedException e1) {
						e1.printStackTrace();
						System.exit(0);
					}
				}

			}
		}

		return null;
	}


	


}
