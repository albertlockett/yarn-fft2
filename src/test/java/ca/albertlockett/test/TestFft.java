package ca.albertlockett.test;

import org.junit.Test;

import ca.albertlockett.Complex;
import ca.albertlockett.Fft;
import ca.albertlockett.test.util.SignalComparator;

public class TestFft {

	private final SignalComparator sC = new SignalComparator();
	private final Fft fft = new Fft();
	
	
	@Test
	public void testSigLen1(){
		
		Complex[] signal = {
				new Complex(1)
		};
		
		Complex[] expect = {
				new Complex(1)
		};
		
		sC.compareSignals(expect, fft.fft(signal));
		
		
		
	}
	
	@Test
	public void testExampleFft1(){
		
		
		Complex[] signal = {
			new Complex(1),
			new Complex(0),
			new Complex(0),
			new Complex(0),
		};
		
		Complex[] fftSig = fft.fft(signal); 
		
		Complex[] expected = {
			new Complex(1),
			new Complex(1),
			new Complex(1),
			new Complex(1),
		};
		
		sC.compareSignals(fftSig, expected);
		
		
	}
	
}
