package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
    private int buckets, min, max;
    private int width;
    private int []size;
    private int totcnt;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets=buckets;
        this.min=min;
        this.max=max;
        size=new int[buckets];
        for (int i=0;i<buckets;++i)
            size[i]=0;
        width = (int) Math.ceil((double) (max - min + 1) / buckets);
        /*if(width*buckets==(max-min+1)){
            width+=1;
        }*/
        totcnt=0;
    }
    private int getindex(int v){
        if(v==max) v--;
        return (v-min+1)/width;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index= getindex(v);
        size[index]+=1;
        totcnt+=1;
    }


    private double calcl(int v){
        int bktid=getindex(v);
        double lsz=0;
        for(int i=0;i<bktid;++i){
            lsz+=size[i];
        }
        lsz+=size[bktid]*((double)(v-bktid*width)/width);
        return lsz*1.0/totcnt;
    }
    private double calcr(int v){
        int bktid=getindex(v);
        double rsz=0;
        for(int i=bktid+1;i<buckets;++i){
            rsz+=size[i];
        }
        rsz+=size[bktid]*((double)(bktid+1)*width-v-1)/width;
        return rsz*1.0/totcnt;
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
        int bktid=getindex(v);
    	// some code goes here
        double l,r;
        switch (op){
            case LIKE: return avgSelectivity();
            case EQUALS: {
                if(v<min || v> max) return 0.0;
                return (1.0/width)*size[bktid]/totcnt;
            }
            case NOT_EQUALS:{
                if(v<min || v> max) return 1.0;
                return 1-(1.0/width)*size[bktid]/totcnt;
            }
            case LESS_THAN:{
                if(v<=min) return 0.0;
                if(v>max) return 1.0;
                l=calcl(v);
                return l;
            }
            case GREATER_THAN:{
                if(v>=max) return 0;
                if(v<min) return 1;
                r=calcr(v);
                return r;
            }
            case LESS_THAN_OR_EQ:{
                return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
            }
            case GREATER_THAN_OR_EQ:{
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
            }
            default:
                throw new RuntimeException("Should not reach hear");
        }
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
        return "to be continued!";
    }
}
