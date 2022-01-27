package com.ksign.wizlook.bigdata.collector.collect.test.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;

public class Telnet2 {
	
	public static void main(String[] args) {
		new Telnet2().exec("10.20.170.160");
	}

	void exec(String ip)
	{
	  Socket sock = null;
	  BufferedReader br = null;
	  PrintWriter pw = null;

	  try
	  {
	    sock = new Socket(ip, 23);

	    br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
	    pw = new PrintWriter(sock.getOutputStream());

	    this.read(br);
	    System.out.println("Sending username");
	    pw.println("wizlook");
	    this.read(br);  // Always blocks here
	    System.out.println("Sending password");
	    pw.println("wizlook");
	    this.read(br);

	    pw.close();
	    br.close();
	    sock.close();
	  }
	  catch(IOException e)
	  {
		  e.printStackTrace();
	  }
	}

	void read(BufferedReader br) throws IOException
	{
	  char[] ca = new char[1024];
	  int rc = br.read(ca);
	  String s = new String(ca).trim();

	  Arrays.fill(ca, (char)0);

	  System.out.println("RC=" + rc + ":" + s);

	//String s = br.readLine();
//	      
	//while(s != null)
	//{
	//  if(s.equalsIgnoreCase("username:"))
//	    break;
//	          
	//  s = br.readLine();
//	          
	//  System.out.println(s);
	//}
	}
}
