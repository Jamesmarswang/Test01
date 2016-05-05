package Mypackage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UDFkey implements WritableComparable<Object> {

	private String firstKey;
	private String secondKey;

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public String getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(String secondKey) {
		this.secondKey = secondKey;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey = in.readUTF();
		this.secondKey = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstKey);
		out.writeUTF(secondKey);
	}

	public boolean equals(Object o) {
		if (o instanceof UDFkey) {
			UDFkey r = (UDFkey) o;
			return (r.firstKey.equals(r.firstKey) && r.secondKey
					.equals(r.secondKey));
		} else {
			return false;
		}

	}

	public int hashcode() {
		return (this.firstKey.hashCode() + this.secondKey.hashCode());
	}

	@Override
	public int compareTo(Object o) {
		// this本对象下载前面代表升序
		// this本对象写在后面为降序
		UDFkey udk = (UDFkey) o;
		return this.getFirstKey().compareTo(udk.getFirstKey());
	}
}
