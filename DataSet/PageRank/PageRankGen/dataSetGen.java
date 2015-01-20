import java.util.Scanner;
import java.io.*;
import java.util.*;
public class dataSetGen {
	
	static int maxConnect;

	public static void main(String [] args){
		Scanner scanner = new Scanner(System.in);
		
		System.out.print("total Node?\t");
		int totalNode = scanner.nextInt();

		System.out.print("start_No?\t");
		int start_No = scanner.nextInt();
		
		System.out.print("end_No?\t\t");
		int end_No = scanner.nextInt();

		System.out.print("maxConnect?\t");
                maxConnect = scanner.nextInt();
		
		produceList(totalNode, start_No, end_No);
	}

	static void produceList(int total, int start, int end){
		int numNode = end-start+1;
		HashMap<Integer,Integer> map;

					

		try{
	
		//create adjList

			String fileName = "/tmp/"+"adjList_" + start + "-" + end + "_" + "link-" + maxConnect;

			FileWriter fwriter = new FileWriter(new File(fileName));
			for(int i = start ; i <= end ; i++){
				map = new HashMap<Integer,Integer>();
				map.put(i,0);
				int connectNode =  (int) (Math.random()*maxConnect + 1);
				//int connectNode = 20 ; 
				String line = i + "\t";
				for(int j = 0 ; j < connectNode ; j++){
					int node;
					do{
						node = (int)(Math.random()*total + 1);
					}while(map.containsKey(node));
					map.put(node, 0);
					line = line + node;
					if( (j+1) != connectNode){
						line = line + ",";
					}
				}
				//System.out.println("log: " +  line);
				fwriter.write(line + "\n");
			}
			fwriter.close();

		//create nodeScore
			fileName = "/tmp/"+"topToRegionFile_" + start + "-" + end;
			fwriter = new FileWriter(new File(fileName));
			String line = null;
			for(int k = start ; k <= end ; k++){
				line = k + "\t" + 1;
				fwriter.write(line + "\n");
			}
			fwriter.close();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}

	}

}
