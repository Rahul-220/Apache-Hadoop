class Elem extends Writable{
  short tag;
  int index;
  double value;
/*......*/
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
    Pair ( int i, int j ) { this.i = i; this.j = j; }

    /*...*/
}

public class Multiply extends Configured implements Tool {

    /* ... */

    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
   	
    }
}
