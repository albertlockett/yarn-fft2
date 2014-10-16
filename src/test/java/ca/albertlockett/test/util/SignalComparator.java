package ca.albertlockett.test.util;

import static org.junit.Assert.*;

import ca.albertlockett.Complex;

public class SignalComparator {

	
	/**
	 * Convenience class to compare two signals
	 * 
	 * @param sig
	 * @param exp
	 */
	public void compareSignals(Complex[] sig, Complex[] exp){
		
		// Test length
		assertEquals("Signals aren't same length", exp.length, sig.length);
		
		// test individual components
		for(int i = 0; i < exp.length; i++){
			assertEquals("Signal compare failed at element "+i + 
					"\nExpected: " + exp[i].real + " + j" + exp[i].imag +
					"\nReceived: " + sig[i].real + " + j" + sig[i].imag + "\n",
				exp[i], sig[i]);
		}
		
	}
	
}
