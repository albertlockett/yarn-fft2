package ca.albertlockett;


/**
 * Class to handle arithmetic operations on complex Numbers. Better to 
 * implement like this so arithmetic operations aren't defined for each
 * complex number
 * 
 * @author albertlockett
 *
 */
public class ComplexOperator {
	
	/**
	 * Arithmetic Multiply
	 * @param c1 
	 * @param c2 
	 * @return c1 *  c2
	 */
	public Complex times(Complex c1, Complex c2){
		
		// Work this out by FOIL
		return new Complex(
				c1.real * c2.real - c1.imag * c2.imag,
				c1.real * c2.imag + c1.imag * c2.real
		);
	}
	
	
	/**
	 * Arithmetic Divide
	 * @param c1 
	 * @param c2 
	 * @return c1 / c2
	 */
	public Complex divide(Complex c1, Complex c2){
		
		return times(c1, recip(c2));
	}
	
	
	/**
	 * Arithmetic Add
	 * @param c1 
	 * @param c2 
	 * @return c1 +  c2
	 */
	public Complex add(Complex c1, Complex c2){
		
		return new Complex(c1.real + c2.real, c1.imag + c2.imag);
	}
	
	
	/**
	 * Arithmetic Subtract
	 * @param c1 
	 * @param c2 
	 * @return c1 - c2
	 */
	public Complex subtract(Complex c1, Complex c2){
		
		return new Complex(c1.real - c2.real, c1.imag - c2.imag);
	}
	
	
	/**
	 * Invert the sign
	 * @param c
	 * @return - c
	 */
	public Complex invert(Complex c){
		
		return new Complex(-1.0 * c.real, -1.0 * c.imag);
	}
	
	
	/**
	 * Calculate reciprocal
	 * @param c 
	 * @return 1 / c
	 */
	public Complex recip(Complex c){
		
		// Do it out on paper, it works
		return new Complex(
			c.real / (c.real * c.real + c.imag * c.imag),
		   -1.0 * c.imag / (c.real * c.real + c.imag * c.imag)
		);
	}

}
