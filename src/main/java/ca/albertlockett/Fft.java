package ca.albertlockett;


/**
 * FFT Class
 * 
 * @author albertlockett
 *
 */
public class Fft {

	private final ComplexOperator co = new ComplexOperator();
	
	/**
	 * Recursively calculate FFT 
	 * 
	 * Based on description available here: 
	 * http://jeremykun.com/2012/07/18/the-fast-fourier-transform/
	 * @param signal
	 * @return FFT of signal
	 */
	public Complex[] fft(Complex[] signal){
		
		int n = signal.length;
		
		if(n == 1){
		
			return signal;
		
		} else {
			
			Complex[] Feven = fft(
					ArraySplitter.getArrayPart(signal, ArraySplitter.FFT.EVEN));
			Complex[] Fodd  = fft(
					ArraySplitter.getArrayPart(signal, ArraySplitter.FFT.ODD));
			
			Complex[] combined = new Complex[n];
			
			for(int m = 0; m < n/2; m++){
				
				combined[m] = co.add(Feven[m], co.times(omega(n,-m), Fodd[m]));
				combined[m + n/2] = co.subtract(Feven[m],
						co.times(omega(n,-m), Fodd[m]));
			}
			
			return combined;
		}
		
	}
	
	
	
	/**
	 * Shorthand for complex exponential
	 * 
	 * w(n,m) = e^(2*pi*j*m/n)
	 * 
	 * @param n demoninator
	 * @param m numerator
	 * @return complex value of e^(2*pi*j*m/n)
	 */
	private Complex omega(int n, int m) {
		
		double x = 2.0 * 3.145 * (double) m / (double) n;
		
		return new Complex( Math.cos(x), Math.sin(x));
	}
	
}
