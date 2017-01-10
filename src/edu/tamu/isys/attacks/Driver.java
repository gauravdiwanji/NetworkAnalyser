package edu.tamu.isys.attacks;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {
	
	public static void main(String[] argv) throws Throwable{
		ProgramDriver myDriver = new ProgramDriver();
		
		int exitCode = -1;
		try{
			myDriver.addClass("arpspoof", Program1.class, "This M/R program detects ARP spoofing.");
			myDriver.addClass("ddos", Program2.class, "This M/R program detects DDoS attacks.");
			myDriver.addClass("hostscan", Program3.class, "This M/R program detects host scans.");
			exitCode = myDriver.run(argv);
		}
		catch (Throwable e){
			e.printStackTrace();
			System.out.print("therez an error");
		}
		System.exit(exitCode);
	}
}

