import ca.albertlockett.Complex;
import ca.albertlockett.Fft;


/**
 * Main Class,
 * probably will just be used now to invoke objects and playing around
 * 
 * @author albertlockett
 */
public class Main {

	public static void main (String[] args){
		
		System.out.println("Welcome to the pig FFT");
		
		Complex[] signal = {
				new Complex(0),
				new Complex(1),
				new Complex(0),
				new Complex(0)
		};
		
		Fft fft = new Fft();
		
		Complex[] signal_fft = fft.fft(signal);
		
		
		for(Complex c : signal_fft){
			System.out.println("" + c.real + " + " + c.imag + "j");
		}
		
	}
	
	
}
