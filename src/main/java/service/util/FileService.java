package service.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.opencsv.CSVReader;

public class FileService {

	private static String columnSeparator = ";";
	private static String lineSeparator = "\n";


	/** ============================================================================================================ */

	public List<String[]> loadCsv(String filename, char separator) {

		try {
			CSVReader reader = new CSVReader(new FileReader(filename), separator);

			List<String[]> csv = reader.readAll();
			reader.close();
			return csv;

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/** ============================================================================= */

	public Map<String, Integer> generateHeaderMap(List<String[]> csv) {
		String[] header = csv.get(0);
		Map<String, Integer> map = new HashMap <String, Integer>();
		for (int i=0; i<header.length; i++) {
			try {
				String h=header[i];
				map.put(h, i);
			}
			catch (Exception e) {
				// nothing to do
			}
		}
		return map;
	}

	/** ============================================================================================================ */

	public void printHeader(List<String[]> csv) {
		String[] header = csv.get(0);
		String [] line1 = csv.get(1);
		for (int i=0; i<header.length; i++) {
			String h = null;
			String l =null;
			try {
				h=header[i];
				l=line1[i];
			}
			catch (Exception e) {
				// nothing to do
			}
			System.out.println(i + ": " + h + "=" + l);
		}
	}

	/** ============================================================================================================ */

	public void printHeader(List<String[]> csv, int lineNumber) {
		String[] header = csv.get(0);
		String [] line1 = csv.get(lineNumber);
		for (int i=0; i<header.length; i++) {
			String h = null;
			String l =null;
			try {
				h=header[i];
				l=line1[i];
			}
			catch (Exception e) {
				// nothing to do
			}
			System.out.println(i + ": " + h + "=" + l);
		}
	}


	/** ============================================================================================================ */

	public void writeCsvFile(String fileName, List<String> header, List<Object> listData) {


		try {
			FileWriter writer = new FileWriter(fileName);

			// === Header ===
			if (header!=null) {
				for (int i=0; i<header.size(); i++) {
					writer.append(header.get(i)!=null ? header.get(i).replaceAll(columnSeparator, "") : "");
					if (i<(header.size()-1)) {
						writer.append(columnSeparator);
					}
				}
				writer.append(lineSeparator);
				writer.flush();
			}

			// === Data ===
			if (listData!=null) {
				for ( Iterator<Object> iterator = listData.iterator(); iterator.hasNext(); ) {
					Object data[] = (Object[]) iterator.next();
					for (int j=0; j<data.length; j++) {
						writer.append(data[j]!=null ? data[j].toString().replaceAll(columnSeparator, "") : "");
						if (j<(data.length-1)) {
							writer.append(columnSeparator);
						}
					}
					writer.append(lineSeparator);
				}
				writer.flush();
			}

			// ===== Close file =====
			writer.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** ============================================================================================================ */

	public void writeCsvFile(String fileName, List<Object> listData, String separator) {


		try {
			FileWriter writer = new FileWriter(fileName);

			// === Data ===
			if (listData!=null) {
				for ( Iterator<Object> iterator = listData.iterator(); iterator.hasNext(); ) {
					Object data[] = (Object[]) iterator.next();
					for (int j=0; j<data.length; j++) {
						writer.append(data[j]!=null ? data[j].toString().replaceAll(separator, "") : "");
						if (j<(data.length-1)) {
							writer.append(separator);
						}
					}
					writer.append(lineSeparator);
				}
				writer.flush();
			}

			// ===== Close file =====
			writer.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** ============================================================================================================ */


	public void writeExcelFile(String fileName, List<String> header, List<Object> listData) {

		// === Blank workbook ===
		XSSFWorkbook workbook = new XSSFWorkbook(); 

		// === Create a blank sheet ===
		XSSFSheet sheet = workbook.createSheet("data");

		// === Nb of rows and cells ===
		int rownum = 0;


		// === Header ===
		if (header!=null) {
			Row row = sheet.createRow(rownum++);
			int cellnum = 0;
			for (int i=0; i<header.size(); i++) {
				Cell cell = row.createCell(cellnum++);
				cell.setCellValue(header.get(i));
			}
		}

		// === Data ===
		if (listData!=null) {
			for ( Iterator<Object> iterator = listData.iterator(); iterator.hasNext(); ) {
				Object data[] = (Object[]) iterator.next();

				Row row = sheet.createRow(rownum++);

				int cellnum = 0;
				for (int j=0; j<data.length; j++) {

					Cell cell = row.createCell(cellnum++);
					cell.setCellType(CellType.STRING);

					boolean isNull = (data[j]==null);
					if (!isNull) {
						cell.setCellValue(data[j].toString());
					}
				}
			}
		}

		// === Write the workbook in file system ===
		try
		{
			FileOutputStream out = new FileOutputStream(new File(fileName));
			workbook.write(out);
			workbook.close();
			out.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}

	}

	/** ================================================================================= */

	public void addSheet(XSSFWorkbook workbook, String sheetName, Collection<String> header, List<Object> listData) {


		// === Create a blank sheet ===
		XSSFSheet sheet = workbook.createSheet(sheetName);

		// === Nb of rows and cells ===
		int rownum = 0;


		// === Header ===
		if (header!=null) {
			Row row = sheet.createRow(rownum++);
			int cellnum = 0;
			for (String item : header) {
				Cell cell = row.createCell(cellnum++);
				cell.setCellValue(item);
			}
		}

		// === Data ===
		if (listData!=null) {
			for ( Iterator<Object> iterator = listData.iterator(); iterator.hasNext(); ) {
				Object data[] = (Object[]) iterator.next();

				Row row = sheet.createRow(rownum++);

				int cellnum = 0;
				for (int j=0; j<data.length; j++) {

					Cell cell = row.createCell(cellnum++);
					cell.setCellType(CellType.STRING);

					boolean isNull = (data[j]==null);
					if (!isNull) {
						cell.setCellValue(data[j].toString());
					}
				}
			}
		}

	}

	/** ================================================================================= */

	public XSSFWorkbook createWorkbook(){
		return new XSSFWorkbook(); 
	}


	/** ================================================================================= */

	public void writeWorkbook(String fileName, XSSFWorkbook workbook) {

		try {
			FileOutputStream out = new FileOutputStream(new File(fileName));
			workbook.write(out);
			workbook.close();
			out.close();

		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
	}

	/** ================================================================================= */

	public List<String> loadText(String inputfile) {
		try {
			List<String> text = new ArrayList<String>();
			BufferedReader br = new BufferedReader(new FileReader(inputfile));
			String line = br.readLine();
			while (line != null) {
				text.add(line.trim());
				line = br.readLine();
			}
			br.close();
			return text;
		} 
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/** ============================================================================================================ */

}
