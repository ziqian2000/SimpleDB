package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int min;
	private int max;
	private int buckets;
	private int interval;
	private int totalNum;
	private int[] left;
	private int[] right;
	private int [] count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
		this.min = min;
		this.max = max;
		this.buckets = Math.min(buckets, max - min + 1);
		left = new int[this.buckets];
		right = new int[this.buckets];
		count = new int[this.buckets];
		this.interval = (max - min + 1) / this.buckets;

		for(int i = 0; i < this.buckets; i++){
			left[i] = min + interval * i;
			right[i] = left[i] + interval - 1;
			count[i] = 0;
		}
		right[this.buckets - 1] = max;
    }

    private int getIdx(int v){
    	assert min <= v && v <= max;
    	int idx = (v - min) / interval;
    	return Math.min(idx, this.buckets - 1);
	}

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
		int idx = getIdx(v);
		count[idx]++;
		totalNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
		switch (op){
			case EQUALS:
				if(v < min || v > max) return 0.0;
				else {
					int idx = getIdx(v);
					return 1.0 * count[idx] / (right[idx] - left[idx] + 1) / totalNum;
				}
			case NOT_EQUALS:
				if(v < min || v > max) return 1.0;
				else{
					int idx = getIdx(v);
					return 1.0 - 1.0 * count[idx] / (right[idx] - left[idx] + 1) / totalNum;
				}
			case LESS_THAN:
			case GREATER_THAN_OR_EQ:
				if(v < min) return op == Predicate.Op.LESS_THAN ? 0.0 : 1.0;
				else if(v > max) return op == Predicate.Op.LESS_THAN ? 1.0 : 0.0;
				else{
					int idx = getIdx(v);
					double sum = 0;
					for(int i = 0; i < idx; i++) sum += count[i];
					sum += 1.0 * count[idx] / (right[idx] - left[idx] + 1) * (v - left[idx]);
					return op == Predicate.Op.LESS_THAN ? sum / totalNum : 1.0 - sum / totalNum;
				}
			case LESS_THAN_OR_EQ:
			case GREATER_THAN:
				if(v < min) return op == Predicate.Op.LESS_THAN_OR_EQ ? 0.0 : 1.0;
				else if(v > max) return op == Predicate.Op.LESS_THAN_OR_EQ ? 1.0 : 0.0;
				else{
					int idx = getIdx(v);
					double sum = 0;
					for(int i = 0; i < idx; i++) sum += count[i];
					sum += 1.0 * count[idx] / (right[idx] - left[idx] + 1) * (v - left[idx] + 1);
					return op == Predicate.Op.LESS_THAN_OR_EQ ? sum / totalNum : 1.0 - sum / totalNum;
				}
		}
		return 2333333;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
