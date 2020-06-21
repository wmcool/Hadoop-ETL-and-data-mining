package utils;

public class Tools {
	/*
	 * compute multidimensional Gaussian probability
	 * 
	 * @param x a point in n-dimensional space
	 * @param mu means of gaussian probability
	 * @param var variance of gaussian probability
	 */
	public static double multi_gaussian(double[] x, double[] mu, double[] cov) {
		int n = x.length;
		double[] error = new double[n];
		double exp_sum = 0;
		double determinant = 1;
		for(int i=0;i<n;i++) {
			error[i] = x[i] - mu[i];
			determinant *= cov[i];
			exp_sum += (error[i] * error[i]) / cov[i];
		}
		exp_sum *= -0.5;
		double fenzi = Math.exp(exp_sum);
		double fenmu = Math.sqrt(Math.pow(2 * Math.PI, n) * determinant);
		double result = fenzi / fenmu;
		return result;
	}
	
	/*
	 * compute product of two vector
	 */
	public static double dot(double[] theta, double[] X) {
		double error = 0.;
		if(theta.length != X.length) {
			return -1;
		}
		for(int i=0;i<theta.length;i++) {
			error += theta[i] * X[i];
		}
		return error;
	}
	
	/*
	 * sigmoid function
	 */
	
	public static double sigmoid(double z) {
		return 1/(1 + Math.exp(-z));
	}
	
	/*
	 * compute Gaussian probability
	 * 
	 * @param x a number in 1-dimension space
	 * @param mu means of gaussian probability
	 * @param var variance of gaussian probability
	 */
	public static double Gaussian(double x, double mu, double var) {
		return (1/(var * Math.sqrt(2 * Math.PI))) * Math.exp(-Math.pow((x - mu), 2) / (2 * Math.pow(var, 2)));
	}
}
