package ca.albertlockett.test;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import ca.albertlockett.Complex;

public class TestComplex {
	
	
	@Test
	public void testDefaultConstructor(){
		
		Complex c = new Complex();
		
		assertEquals(new Double(0.0), c.real);
		
		
	}
	
	
}
