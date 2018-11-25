package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> agcnt;
    private TupleDesc td;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        if(what!=Op.COUNT){
            throw new UnsupportedOperationException("String only support COUNT aggregation");
        }
        agcnt=new HashMap<>();
        Type[] types;
        String[] names;
        if(gbfield==-1){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"123"};
        }
        else {
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            names = new String[]{"123", "456"};
        }
        td = new TupleDesc(types, names);
    }
    public TupleDesc getdesc(){
        return td;
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field af=tup.getField(afield), gf=null;
        //Integer val=((IntField)af).getValue();
        if(gbfield!=-1) {
            gf = tup.getField(gbfield);
        }
        if(!agcnt.containsKey(gf)){
            agcnt.put(gf, 1);
        }
        else {
            agcnt.put(gf, agcnt.get(gf)+1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> item : agcnt.entrySet()){
            Tuple t = new Tuple(td);
            Integer val=item.getValue(), cnt=agcnt.get(item.getKey());
            if(gbfield==-1){
                t.setField(0, new IntField(val));
            }
            else {
                t.setField(0, item.getKey());
                t.setField(1, new IntField(val));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
