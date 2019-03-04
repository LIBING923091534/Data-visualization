package org.zyt.taxi.tp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TpWritable implements WritableComparable<TpWritable> {

	private Text id = new Text("");
	private Long timestamp = new Long(0);
	private Long lng = new Long(0);
	private Long lat = new Long(0);
	private Integer status = 0;

	public TpWritable(Text id, Long timestamp, Long lng, Long lat, Integer status) {
		this.id = id;
		this.timestamp = timestamp;
		this.lng = lng;
		this.lat = lat;
		this.status = status;
	}
	
	public TpWritable(){
		
	}
	
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		this.timestamp = in.readLong();
		this.lng = in.readLong();
		this.lat = in.readLong();
		this.status = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		out.writeLong(timestamp);
		out.writeLong(lng);
		out.writeLong(lat);
		out.writeInt(status);
	}

	public int compareTo(TpWritable tp) {
		return this.timestamp > tp.getTimestamp() ? 1 : 0;
	}

	public Text getId() {
		return id;
	}

	public TpWritable setId(Text id) {
		this.id = id;
		return this;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public TpWritable setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	public Long getLng() {
		return lng;
	}

	public TpWritable setLng(Long lng) {
		this.lng = lng;
		return this;
	}

	public Long getLat() {
		return lat;
	}

	public TpWritable setLat(Long lat) {
		this.lat = lat;
		return this;
	}

	public Integer getStatus() {
		return status;
	}

	public TpWritable setStatus(Integer status) {
		this.status = status;
		return this;
	}
	
	@Override
	public String toString() {
		return id.toString()+' '+timestamp+' '+lng+' '+lat+' '+status;
	}
	
}
