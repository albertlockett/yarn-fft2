package ca.albertlockett.test;

import org.junit.Test;

import ca.albertlockett.ArraySplitter;
import ca.albertlockett.Complex;
import ca.albertlockett.test.util.SignalComparator;


/**
 * Test class for ArraySplitter
 * 
 * @author albertlockett
 *
 */
public class TestArraySplitter {

	private final SignalComparator sC = new SignalComparator();
	
	@Test
	public void smokeTest(){
		
		// Smoke test 1:
		Complex[] smokeTest1Signal = {
				new Complex(1),
				new Complex(0),
				new Complex(2),
				new Complex(0)
		};
		
		// Test Even
		Complex[] fEven = ArraySplitter.getArrayPart(
				smokeTest1Signal,
				ArraySplitter.FFT.EVEN);
		Complex[] smokeTestEvenExp =  {
				smokeTest1Signal[0], 
				smokeTest1Signal[2]
			};
		sC.compareSignals(fEven, smokeTestEvenExp);
		
		// Test Odd
		Complex[] fOdd = ArraySplitter.getArrayPart(
				smokeTest1Signal,
				ArraySplitter.FFT.ODD);
		Complex[] smokeTestOddExp = {
				smokeTest1Signal[1], 
				smokeTest1Signal[3]
			};
		sC.compareSignals(fOdd, smokeTestOddExp);
		
		
		// Test a longer more complex signal
		Complex[] smokeTest2Signal = {
				new Complex(1.2, 1.6),		// 0
				new Complex(1.2, 1.4),		// 1
				new Complex(1.1, 1.7),		// 2
				new Complex(1.2, 1.6), 		// 3
				new Complex(5.2, 1.0), 		// 4
				new Complex(1.2, 1.6), 		// 5
				new Complex(-1.2, 1.6), 	// 6
				new Complex(1.2, 1.6), 		// 7
				new Complex(-1.2, 1.6),		// 8
				new Complex(99.0, 1.6) 		// 9
		};
		
		fEven = ArraySplitter.getArrayPart(
				smokeTest2Signal,
				ArraySplitter.FFT.EVEN);
		Complex[] smokeTest2EvenExp = {
			smokeTest2Signal[0],
			smokeTest2Signal[2],
			smokeTest2Signal[4],
			smokeTest2Signal[6],
			smokeTest2Signal[8],
		};
		sC.compareSignals(fEven, smokeTest2EvenExp);
		
		fOdd = ArraySplitter.getArrayPart(
				smokeTest2Signal,
				ArraySplitter.FFT.ODD);
		Complex[] smokeTest2OddExp = {
			smokeTest2Signal[1],
			smokeTest2Signal[3],
			smokeTest2Signal[5],
			smokeTest2Signal[7],
			smokeTest2Signal[9],
		};
		sC.compareSignals(fOdd, smokeTest2OddExp);
		
		
	}
	
	
}
