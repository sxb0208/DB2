package com.db2;

import java.io.*;
import java.util.*;

public class TransStart {
	public static HashMap<Integer, Transaction> transMap = new HashMap<Integer, Transaction>();
	public static HashMap<String, LockTable> lockMap = new HashMap<String, LockTable>();
	public static String[] readData = null;
	public static LinkedList<String> waitTransactionList = new LinkedList<String>();
	public static HashSet<Integer> waitTransactionid = new HashSet<Integer>();

	public static void main(String args[]) throws IOException {
		TransStart transStartObj = new TransStart();
		readData = transStartObj.readFile();
		System.out.println("Rigorous 2PL implementation using wait-die method for dealing with deadlocks\n");
		TransactionProcess process = new TransactionProcess();
		process.readTransactions();
		System.exit(0);
	}

	public String[] readFile() throws IOException {
		BufferedReader in = new BufferedReader(new FileReader("E:\\Courses\\Summer-2017\\DB2\\Project 1\\Inputs\\input5.txt"));
		String str=null;
		ArrayList<String> lines = new ArrayList<String>();
		while((str = in.readLine()) != null){
		    lines.add(str.replaceAll("\\s",""));
		}
		String[] linesArray = lines.toArray(new String[lines.size()]);
		return linesArray;
	}
}