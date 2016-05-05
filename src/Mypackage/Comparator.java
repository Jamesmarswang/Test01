package Mypackage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Comparator extends WritableComparator {

	protected Comparator() {
		super(UDFkey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		UDFkey t1 = (UDFkey) a;
		UDFkey t2 = (UDFkey) b;
		int temp = t1.getFirstKey().compareTo(t2.getFirstKey());
		if (temp == 0) {
			return t1.getSecondKey().compareTo(t2.getSecondKey());
		}
		return temp;
	}
}
