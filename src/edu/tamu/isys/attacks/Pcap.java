package edu.tamu.isys.attacks;

import java.io.FileReader;
import java.io.StringReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Pcap {
	private int No;
    private double Time;
    private String Source;
    private String Destination;
    private String Protocol;
    private int Length;
    private String Info;
     
    public int getNo() {
        return No;
    }
    public void setNo(String no) {
        try
        {this.No = Integer.parseInt(no);}
        catch(Exception e)
        {this.No = -1;};
    }
    
    public double getTime() {
        return Time;
    }
    public void setTime(String time) {
    	try
        {this.Time = Double.parseDouble(time);}
        catch(Exception e)
        {this.Time = -1;};
    }
    public String getSource() {
        return Source;
    }
    public void setSource(String source) {
        this.Source = source;
    }
    
    public String getDestination() {
        return Destination;
    }
    public void setDestination(String destination) {
        this.Destination = destination;
    }
    
    public String getProtocol() {
        return Protocol;
    }
    public void setProtocol(String protocol) {
        this.Protocol = protocol;
    }
    
    public int getLength() {
        return Length;
    }
    public void setLength(String length) {
        try
        {this.Length = Integer.parseInt(length);}
        catch(Exception e)
        {this.Length = -1;};
    }
    
    public String getInfo() {
        return Info;
    }
    public void setInfo(String info) {
        this.Info = info;
    }
    
   public int writeFromCSV(String s)
   {		System.out.println("Pcap Parser Value: "+s);
   			String header = "\"No.\",\"Time\",\"Source\",\"Destination\",\"Protocol\",\"Length\",\"Info\"\n";
			CSVFormat format = CSVFormat.EXCEL.withHeader();
			try
			{
			   CSVParser parser = new CSVParser(new StringReader(header+s), format);	
			   
			   for(CSVRecord record : parser){
			   	this.setNo(record.get("No."));
			   	this.setTime(record.get("Time"));
			   	this.setSource(record.get("Source"));
			   	this.setDestination(record.get("Destination"));
			   	this.setProtocol(record.get("Protocol"));
			   	this.setLength(record.get("Length"));
			   	this.setInfo(record.get("Info"));
			   }
			   parser.close();
			   System.out.println("Value is: "+No+" "+ Time+" "+Source+" "+Destination+" "+Protocol+" "+Length+ " "+Info);
			   if (this.Time != -1)
			   {return 1;}
			   else
			   {return 0;}
			}
			catch (Exception e)
			{System.out.println(e.getMessage());
				return 0;}
   }
   
    @Override
    public String toString(){
        return (No+" "+ Time+" "+Source+" "+Destination+" "+Protocol+" "+Length+ " "+Info);
 }

}
