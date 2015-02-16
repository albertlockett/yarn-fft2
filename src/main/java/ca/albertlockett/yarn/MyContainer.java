package ca.albertlockett.yarn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.albertlockett.Complex;
import ca.albertlockett.Fft;

public class MyContainer {

	private static final Logger log = 
			LoggerFactory.getLogger(MyContainer.class);
	
	private String hostname;
	private YarnConfiguration conf;
	private FileSystem fs;
	private Path inputFile;
	private Path outputFile;
	private long start;
	private int length;
	
	public MyContainer(String[] args) throws IOException {
		
		this.hostname = NetUtils.getHostname();
		this.conf = new YarnConfiguration();
		this.fs = FileSystem.get(conf);
		
		this.inputFile = new Path(args[0]);
		this.outputFile = new Path(args[1]);
		this.start = Long.parseLong(args[2]);
		this.length = Integer.parseInt(args[3]);
		
	}
	
	
	public void run() throws IOException {
		
		log.info("Running container on {}", this.hostname);
		
		// Setup input
		FSDataInputStream fsdis = fs.open(this.inputFile);
		fsdis.seek(this.start);
		
		BufferedReader reader = 
				new BufferedReader(new InputStreamReader(fsdis));
		
		// Setup output
		FSDataOutputStream fsout = this.fs.create(outputFile);
		
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(fsout));

		// perform container logic
		log.info("Reading {} from {} to {}", inputFile.toString(), start, 
				start + length);

		// Create the signal from input
		List<Complex> signal = new ArrayList<Complex>();
		long bytesRead = 0;
		String current = "";
		while(bytesRead < this.length &&
				(current = reader.readLine()) != null) {
			Integer sample = Integer.parseInt(current);
			signal.add(new Complex(sample));
		}
		
		// process signal - In chunks of sampling rate
		// TODO: Add actual sampling rate as arguement
		Fft fft = new Fft();
		int sampling_rate = 44100;

		
		Complex[] fft_sig = new Complex[signal.size()];
		for( int i=0; i < signal.size(); i++ ) {
			fft_sig[i] = signal.get(i);
		}
		fft_sig = fft.fft(fft_sig);
		
		for( Complex c : fft_sig ) {
			writer.append(c.toString());
			writer.append(",");
		}
		
		
		// close IO
		reader.close();
		fsdis.close();
		writer.close();
		fsout.close();
		
	}
	
	public static void main(String[] args) {
		
		log.info("PREPARING TO RUN CONTAINER");
		
		MyContainer container;
		
		try{
			container = new MyContainer(args);
			container.run();
		} catch(IOException e){
			log.error("Exception caught running Container");
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
		log.info("CONTAINER RAN");
	}
	
	
	
}
