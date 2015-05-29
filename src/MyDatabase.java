import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;


public class MyDatabase {

	private static Map<String,String> columnNames = new LinkedHashMap<String,String>();
	private static List<String> compOperator = new ArrayList<String>();

	private static Map<String,Map<String,List<Long>>> indicesMap = new HashMap<String,Map<String,List<Long>>>();



	public static void main(String[] args) throws FileNotFoundException,IOException,ClassNotFoundException{

		createColumnNames();
		String ans=null,field=null,compOp=null,columnValue=null;;

		while(true){
			System.out.println("Do you want to create a binary file (yes/no)?");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			ans = br.readLine();

			if(!"YES".equals(ans.toUpperCase()) &&  !"NO".equals(ans.toUpperCase())){
				System.out.println("Invalid input");

			}else{
				break;
			}

		}

		while(true){
			System.out.println("Choose a field to query."+columnNames.keySet());
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			field = br.readLine();
			if(!columnNames.keySet().contains(field)){
				System.out.println("Invalid input");

			}else{
				break;
			}

		}

		while(true){
			System.out.println("Choose a comparison operator to query."+compOperator +". \nFor boolean values only = operator is allowed.");

			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			compOp = br.readLine();
			if(!compOperator.contains(compOp)){
				System.out.println("Invalid input");

			}else{
				break;
			}

		}

		System.out.println("Enter column value.");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		columnValue = br.readLine();



		List<String[]> csvFile=readCSV("PHARMA_TRIALS_1000B.csv");
		File file=new File("data.db");
		if(!file.exists() && "NO".equals(ans.toUpperCase())){
			System.out.println("Binary file does not exist. Do you want to create a binary file now? If you choose no , application will exit");
			ans = br.readLine();
			if(!"YES".equals(ans.toUpperCase())){
				System.exit(0);
			}
		}
		if("YES".equals(ans.toUpperCase())){
			writeBinaryFile(csvFile);
			saveIndices();
		}


		Map<String,List<Long>> indexFile = readIndices(field);

		readRecord(columnValue,field,compOp,indexFile);

	}

	private static void createColumnNames() {
		columnNames.put("id","int");
		columnNames.put("company","string");
		columnNames.put("drug_id","string");
		columnNames.put("trials","int");
		columnNames.put("patients","int");
		columnNames.put("dosage_mg","int");
		columnNames.put("reading","float");
		
		columnNames.put("double_blind","string");
		columnNames.put("controlled_study","string");
		columnNames.put("govt_funded","string");
		columnNames.put("fda_approved","string");


		compOperator.add("=");
		compOperator.add("<>");
		compOperator.add("<");
		compOperator.add("<=");
		compOperator.add(">");
		compOperator.add(">=");
	}

	private static List<String[]> readCSV(String csvFileName) throws IOException {
		List<String[]> csvData = new ArrayList<String[]>();

		CSVReader reader = new CSVReader(new FileReader(csvFileName), ',' , '"' , 1);
		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			if (nextLine != null) {
				csvData.add(nextLine);
			}
		}
		reader.close();
		return csvData;

	}

	private static void writeBinaryFile(List<String[]> csvFile) throws FileNotFoundException,IOException{
		RandomAccessFile randomAccessFile = new RandomAccessFile("data.db", "rw");
		int id=0;
		String companyName=null,drugId=null,lastFourColumns=null;
		short trails =0,patients=0,dosageMg;
		long recordPointer=0;
		float reading;
		for(String[] line : csvFile){

			recordPointer=randomAccessFile.getFilePointer();


			id=Integer.parseInt(line[0]);
			addIndices("id", String.valueOf(id), recordPointer);
			randomAccessFile.writeInt(id);

			companyName=line[1];
			addIndices("company", companyName, recordPointer);
			randomAccessFile.write((byte)companyName.length());
			randomAccessFile.write(companyName.getBytes());

			drugId=line[2];
			addIndices( "drug_id",drugId, recordPointer);
			randomAccessFile.write(drugId.getBytes());

			trails=Short.parseShort(line[3]);
			addIndices("trials", String.valueOf(trails), recordPointer);
			randomAccessFile.writeShort(trails);

			patients=Short.parseShort(line[4]);
			addIndices("patients",String.valueOf(patients), recordPointer);
			randomAccessFile.writeShort(patients);

			dosageMg=Short.parseShort(line[5]);
			addIndices("dosage_mg", String.valueOf(dosageMg), recordPointer);
			randomAccessFile.writeShort(dosageMg);

			reading= Float.parseFloat(line[6]);
			addIndices("reading",String.valueOf(reading), recordPointer);
			randomAccessFile.writeFloat(reading);

			lastFourColumns="0000";

			addIndices("double_blind", line[7], recordPointer);
			addIndices("controlled_study", line[8], recordPointer);
			addIndices("govt_funded", line[9], recordPointer);
			addIndices("fda_approved", line[10], recordPointer);
			for(int k=7;k<=10;k++){
				if("TRUE".equals(line[k].toUpperCase())){
					lastFourColumns=lastFourColumns.concat("1");
				}else{
					lastFourColumns=lastFourColumns.concat("0");
				}
			}

			byte b = Byte.parseByte(lastFourColumns, 2);
			randomAccessFile.write(b);
		}

		randomAccessFile.close();
	}


	private static void addIndices(String columnName,String columnVal,long pointer){
		
		if(indicesMap.get(columnName)==null){

			Map<String,List<Long>> index= new HashMap<String,List<Long>>();
			List<Long> recordPointer = new ArrayList<Long>();
			recordPointer.add(pointer);
			index.put(columnVal, recordPointer);
			indicesMap.put(columnName, index);


		}else if (indicesMap.get(columnName).get(columnVal)==null){
			List<Long> recordPointer = new ArrayList<Long>();
			recordPointer.add(pointer);
			indicesMap.get(columnName).put(columnVal, recordPointer);
		}
		else{
			indicesMap.get(columnName).get(columnVal).add(pointer);
		}

	}

	private static void readRecord(String columnVal,String columnName,String compOp,Map<String,List<Long>> indexFile) throws FileNotFoundException,IOException{
		if(indexFile==null){
			System.out.println("Invalid Field Name");
			return;
		}
		
		if("=".equals(compOp) && indexFile.get(columnVal)==null){
			System.out.println("No Data");
			return;
		}

		RandomAccessFile randomAccessFile = new RandomAccessFile("data.db", "rw");
		StringBuilder records = new StringBuilder();
		List<Long> requiredFields = null;

		if("string".equals(columnNames.get(columnName))){
			requiredFields=getrequiredStringrecords(columnVal,columnName,compOp,indexFile);
		}else if("int".equals(columnNames.get(columnName))){
			requiredFields=getrequiredIntrecords(columnVal,columnName,compOp,indexFile);
		}else{
			requiredFields=getrequiredFloatrecords(columnVal,columnName,compOp,indexFile);
		}

		if(requiredFields.size()==0){
			System.out.println("No Data .");
			randomAccessFile.close();
			return;
		}
		for(long recordPointer:requiredFields){
			randomAccessFile.seek(recordPointer);
			records.append(randomAccessFile.readInt()+" | ");
			int length=(int)randomAccessFile.read();
			byte[] byteArray = new byte[length];
			randomAccessFile.read(byteArray);
			records.append(new String(byteArray,"UTF-8")+" | ");
			byteArray = new byte[6];
			randomAccessFile.read(byteArray);
			records.append(new String(byteArray,"UTF-8")+" | ");
			records.append(randomAccessFile.readShort()+" | ");
			records.append(randomAccessFile.readShort()+" | ");
			records.append(randomAccessFile.readShort()+" | ");
			records.append(randomAccessFile.readFloat()+" | ");


			String binayString = Integer.toBinaryString(randomAccessFile.readByte());
			String padding="";
			if(binayString.length()<4){
				for(int k=0;k<(4-binayString.length());k++){
					padding+="0";
				}
				binayString=padding+binayString;
			}

			for(int k=0;k< 4;k++){
				if('1'== binayString.charAt(k)){
					records.append("true | ");
				}else{
					records.append("false | ");
				}
			}
			records.append("\n");
		}

		randomAccessFile.close();
		System.out.println(records.toString());
		System.out.println("Total Records : "+requiredFields.size());
	}


	private static List<Long> getrequiredIntrecords( String columnValString,String columnName,String compOp,Map<String,List<Long>> indexFile) throws FileNotFoundException,IOException{
		List<Long> requiredFields = new ArrayList<Long>();
		if("=".equals(compOp)){
			requiredFields.addAll(indexFile.get(columnValString));
		}
		int columnVal = Integer.parseInt(columnValString);
		if("<>".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((int)columnVal != Integer.parseInt(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if("<".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((int)columnVal > Integer.parseInt(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if("<=".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((int)columnVal >= Integer.parseInt(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if(">".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((int)columnVal < Integer.parseInt(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if(">=".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((int)columnVal <= Integer.parseInt(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		return requiredFields;
	}

	private static List<Long> getrequiredFloatrecords( String columnValString,String columnName,String compOp,Map<String,List<Long>> indexFile) throws FileNotFoundException,IOException{
		List<Long> requiredFields = new ArrayList<Long>();
		if("=".equals(compOp)){
			requiredFields.addAll(indexFile.get(columnValString));
		}
		Float columnVal = Float.parseFloat(columnValString);
		if("<>".equals(compOp)){
			for(String key : indexFile.keySet()){
				if(columnVal != Float.parseFloat(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if("<".equals(compOp)){
			for(String key : indexFile.keySet()){
				if(columnVal > Float.parseFloat(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if("<=".equals(compOp)){
			for(String key : indexFile.keySet()){
				if(columnVal >= Float.parseFloat(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if(">".equals(compOp)){
			for(String key : indexFile.keySet()){
				if(columnVal < Float.parseFloat(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		else if(">=".equals(compOp)){
			for(String key : indexFile.keySet()){
				if(columnVal <= Float.parseFloat(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		return requiredFields;
	}

	private static List<Long> getrequiredStringrecords(String columnVal,String columnName,String compOp,Map<String,List<Long>> indexFile){
		List<Long> requiredFields = new ArrayList<Long>();
		if("=".equals(compOp)){
			requiredFields.addAll(indexFile.get(columnVal));
		}

		if("<>".equals(compOp)){
			for(String key : indexFile.keySet()){
				if(!columnVal.equals(key)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		if("<".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((columnVal).compareTo(key) > 0){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		if("<=".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((columnVal).compareTo(key) > 0 || (columnVal).compareTo(key) == 0){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		if(">".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((columnVal).compareTo(key) < 0){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		if(">=".equals(compOp)){
			for(String key : indexFile.keySet()){
				if((columnVal).compareTo(key) < 0 || (columnVal.compareTo(key) == 0)){
					requiredFields.addAll(indexFile.get(key));
				}
			}
		}
		return requiredFields;
	}


	private static void saveIndices(){
		try{
			// Serialize data object to a file



			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("indices.ndx"));
			out.writeObject(indicesMap);
			out.close();

		} catch (IOException e) {
		}
	}


	private static Map<String, List<Long>> readIndices(String columnName) throws IOException,ClassNotFoundException{

		FileInputStream door = new FileInputStream("indices.ndx"); 
		ObjectInputStream reader = new ObjectInputStream(door); 
		Map<String,Map<String,List<Long>>> indexFile = (Map<String,Map<String,List<Long>>>) reader.readObject(); 

		reader.close();
		return indexFile.get(columnName);

	}


}
