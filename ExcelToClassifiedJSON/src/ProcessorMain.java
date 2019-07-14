import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONArray;
import org.json.JSONObject;

public class ProcessorMain 
{
	public static void main(String[] args) throws IOException
	{
		Reader in = new FileReader("trainutf8.csv");
		Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
		
		BufferedWriter writer = Files.newBufferedWriter(Paths.get("output.json"));
		
		int cnt = 0;
		
		JSONObject head = new JSONObject();
		JSONArray toxicArray = new JSONArray();
		JSONArray obsceneArray = new JSONArray();
		JSONArray insultArray = new JSONArray();
		JSONArray hatespeechArray = new JSONArray();
		JSONArray threatArray = new JSONArray();
		JSONArray cleanArray = new JSONArray();
		
		head.put("CLEAN", cleanArray);
		head.put("TOXIC", toxicArray);
		head.put("OBSCENE", obsceneArray);
		head.put("INSULT", insultArray);
		head.put("THREAT", threatArray);
		head.put("HATE_SPEECH", hatespeechArray);
		
		
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
			boolean threat = record.get("threat").equals("1");
			System.out.print(cnt + ") Added ID " + id + " as ");
			
			ArrayList<String> attributeList = new ArrayList<String>();
			
			boolean owo = false;
			
			if (toxic || toxic_severe)
			{
				attributeList.add("TOXIC");
				toxicArray.put(comment);
				owo = true;
			}
			
			if (obscene)
			{
				attributeList.add("OBSCENE");
				obsceneArray.put(comment);
				owo = true;
			}
			
			if (insult)
			{
				attributeList.add("INSULT");
				insultArray.put(comment);
				owo = true;
			}
			
			if (identity_hate)
			{
				attributeList.add("HATE_SPEECH");
				hatespeechArray.put(comment);
				owo = true;
			}
			
			if (threat)
			{
				attributeList.add("THREAT");
				threatArray.put(comment);
				owo = true;
			}
			
			if (!owo)
			{
				// clean
				cleanArray.put(comment);
			}
			
			if (attributeList.size() == 0)
			{
				System.out.println("CLEAN");
			}
			else
			{
				for (int i = 0; i < attributeList.size(); i++)
				{
					if (i != 0)
					{
						System.out.print(", ");
					}
					System.out.print(attributeList.get(i));
				}
				System.out.println();
			}
			
			++cnt;
		}
		
		writer.write(head.toString(4) + "\n");
		writer.close();
		System.out.println(head.toString(4));
		
		
	}
}
