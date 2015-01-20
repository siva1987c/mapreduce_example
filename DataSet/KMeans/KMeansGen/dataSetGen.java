import java.util.Scanner;
import java.io.*;
class dataSetGen {
	public static void main(String [] args){
		long Dim = 0;
		long vecNum = 0;
		long min = 0;
		long max = 1000;
		Scanner scanner = new Scanner(System.in);
		System.out.print("Dimension? ");
		Dim = scanner.nextInt();
		System.out.print("Vecter number? ");
		vecNum = scanner.nextLong();
		System.out.print("min? ");
		min = scanner.nextInt();
		System.out.print("max? ");
		max = scanner.nextInt();
		String fileName = "d" + Dim + "_" + "v" + vecNum + "_" + min + "to" + max;
		try{
			FileWriter fwriter = new FileWriter(new File(fileName));
			for(long i = 0 ; i < vecNum ; i++){
				for(long j = 0 ; j < Dim ; j++){
					//int randNum = (int)(Math.random()*1000+1);
					long randNum = (long)(Math.random()*(max-min)+min);
					if(j+1 != Dim)
						fwriter.write(randNum + ",");
					else
						fwriter.write(randNum + "\n");
				}
			}
			fwriter.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
