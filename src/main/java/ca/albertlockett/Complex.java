package ca.albertlockett;

/**
 * Custom implementation of complex number class
 * TODO: Fix the -0.0 problem.. It breaks the tests
 * 
 * @author albertlockett
 *
 */
public class Complex {
	
	public Double real;
	public Double imag;
	
	// Constructors
	/**
	 * @return 0 + 0j
	 */
	public Complex(){
		this.real = 0.0;
		this.imag = 0.0;
	}
	
	/**
	 * @param real
	 * @param imag
	 * @return real + imag * j
	 */
	public Complex(Double real, Double imag){
		this.real = real;
		this.imag = imag;
	}
	
	/**
	 * @param real
	 * @return real + 0j
	 */
	public Complex(Double real){
		this.real = real;
		this.imag = 0.0;
	}
	
	/**
	 * @param real
	 * @param imag
	 * @return real + imag * j
	 */
	public Complex(int real, int imag){
		this.real = (double) real;
		this.imag = (double) imag;
	}
	
	/**
	 * @param real
	 * @return real + 0j
	 */
	public Complex(int real){
		this.real = (double) real;
		this.imag = 0.0;
	}
	
	
	/**
	 * This method makes testing easier
	 */
	@Override
	public boolean equals(Object c){
		if(	this.real.equals(((Complex) c).real) && 
			this.imag.equals(((Complex)c).imag)) 
		{
			return true;
		} 
		return false;
	}

	

}
