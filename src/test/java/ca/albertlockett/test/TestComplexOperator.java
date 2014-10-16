package ca.albertlockett.test;

import static org.junit.Assert.*;

import org.junit.Test;

import ca.albertlockett.Complex;
import ca.albertlockett.ComplexOperator;

public class TestComplexOperator {

	private final ComplexOperator cop = new ComplexOperator();
	
	
	@Test
	public void testTimes(){
		
		// 1+0j * 1+0j
		assertEquals(
			cop.times(new Complex(1,0), new Complex(1,0)),
			new Complex(1,0)
		);
		
		// 0+1j * 0+1j
		assertEquals(
			cop.times(new Complex(0,1), new Complex(0,1)),	
			new Complex(-1,0)
		);
		
		// 2+1j * 2+1j
		assertEquals(
			cop.times(new Complex(2,1), new Complex(2,1)),	
			new Complex(3,4)
		);
		
	}
	
	@Test
	public void testDivide(){
		
		// 1+0j / 1+0j
		assertEquals(
			cop.divide(new Complex(1,0), new Complex(1,0)),
			new Complex(1,0)
		);
		
		// 0+1j / 0+1j
		assertEquals(
			cop.divide(new Complex(0,1), new Complex(0,1)),
			new Complex(1,0)
		);
		
		// 0+1j / 0+1j
		assertEquals(
			cop.divide(new Complex(3,4), new Complex(2,1)),
			new Complex(2,1)
		);
		
	}
	
	
	@Test
	public void testAdd(){
		
		// 1+0j + 1+0j
		assertEquals(
			cop.add(new Complex(1,0), new Complex(1,0)),
			new Complex(2,0)
		);
		
		// 1+1j + 1+1j
		assertEquals(
			cop.add(new Complex(1,1), new Complex(1,1)),
			new Complex(2,2)
		);
		
		
	}
	
	@Test
	public void testSubtract(){

		// 1+0j - 1+0j
		assertEquals(
			cop.subtract(new Complex(1,0), new Complex(1,0)),
			new Complex(0,0)
		);
		
		// 1+1j - 1+1j
		assertEquals(
			cop.subtract(new Complex(1,1), new Complex(1,1)),
			new Complex(0,0)
		);
	}
	
	
	@Test
	public void testInvert(){
		
		// -x, x = 1 + 0j
		assertEquals(cop.invert(new Complex(1,0)), new Complex(-1.0, -0.0));
		
		
	}
	
	@Test
	public void testRecip(){
		
		// 1/(1+1j)
		assertEquals(cop.recip(new Complex(1.0,1.0)), new Complex(0.5,-0.5));
	}
	
	
}
