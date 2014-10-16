package ca.albertlockett;

import java.util.ArrayList;
import java.util.List;

/**
 * I split this out into a seperate class so I could test it more easily
 * 
 * @author albertlockett
 *
 */
public class ArraySplitter {
	
	/**
	 * Splits signal array into even or odd elements based on part enum
	 * @param signal signal to be split
	 * @param part FFT either even or odd
	 * @return either all even values or all odd values of signal
	 */
	public static Complex[] getArrayPart(Complex[] signal, FFT part){
		
		// Temporarily hold signal
		List<Complex> outputSignalContainer = new ArrayList<Complex>();
		
		// EVEN/ODD Modifier
		int x = 1;
		if (FFT.EVEN == part){
			x = 0;
		}
		
		// Add correct signal components into container
		for(int i = 0; i < signal.length; i++){
			if((i + x) % 2 == 0){
				outputSignalContainer.add(signal[i]);
			}
		}
		
		// Transform container contents into signal
		Complex[] outputSignal = new Complex[outputSignalContainer.size()];
		for(int i = 0; i < outputSignal.length; i++){
			outputSignal[i] = outputSignalContainer.get(i);
		}
		
		
		return outputSignal;
	}

	/**
	 * Used to choose the whether the partial array should return even or odd
	 * values of the signal
	 * @author albertlockett
	 *
	 */
	public static enum FFT{
		EVEN, ODD;
	}
}
