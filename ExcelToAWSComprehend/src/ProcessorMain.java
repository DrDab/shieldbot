import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class ProcessorMain 
{
	public static void main(String[] args) throws IOException
	{
		Reader in = new FileReader("trainutf8.csv");
		Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
		
		BufferedWriter writer = Files.newBufferedWriter(Paths.get("output.csv"));
		CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
		
		int cnt = 0;
		for (CSVRecord record : records) 
		{
			String id = record.get("﻿id");
			String comment = record.get("comment_text");
			/*
			if (comment.length() > 5000)
			{
				System.out.println("Comment size is bigger than limit. Trimming to 5000 chars. (" + comment.length() + ")");
				comment = comment.substring(0, 5001);
			}
			*/
			boolean toxic = record.get("toxic").equals("1");
			boolean toxic_severe = record.get("severe_toxic").equals("1");
			boolean obscene = record.get("obscene").equals("1");
			boolean insult = record.get("insult").equals("1");
			boolean identity_hate = record.get("identity_hate").equals("1");
			System.out.print(cnt + ") Added ID " + id + " as ");
			
			ArrayList<String> attribList = new ArrayList<String>();
			
			boolean owo = false;
			
			if (toxic || toxic_severe)
			{
				attribList.add("TOXIC");
				csvPrinter.printRecord("TOXIC", comment);
				owo = true;
			}
			
			if (obscene)
			{
				attribList.add("OBSCENE");
				csvPrinter.printRecord("OBSCENE", comment);
				owo = true;
			}
			
			if (insult)
			{
				attribList.add("INSULT");
				csvPrinter.printRecord("INSULT", comment);
				owo = true;
			}
			
			if (identity_hate)
			{
				attribList.add("HATE_SPEECH");
				csvPrinter.printRecord("HATE_SPEECH", comment);
				owo = true;
			}
			
			if (!owo)
			{
				csvPrinter.printRecord("CLEAN", comment);
			}
			
			if (attribList.size() == 0)
			{
				System.out.println("CLEAN");
			}
			else
			{
				for (int i = 0; i < attribList.size(); i++)
				{
					if (i != 0)
					{
						System.out.print(", ");
					}
					System.out.print(attribList.get(i));
				}
				System.out.println();
			}
			
			csvPrinter.flush();
			++cnt;
		}
	}
}
