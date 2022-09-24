package org.apache.flink.runtime.tornadovm;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 */
public class GroupByData {

	private int[] numberOfElementsPerGroup;
	private HashMap<Integer, ArrayList<byte[]>> bytesOfGroup;

	public GroupByData (int[] numberOfElementsPerGroup, HashMap<Integer, ArrayList<byte[]>> bytesOfGroup) {
		this.numberOfElementsPerGroup = numberOfElementsPerGroup;
		this.bytesOfGroup = bytesOfGroup;
	}

	public int[] getNumberOfElementsPerGroup() {
		return numberOfElementsPerGroup;
	}

	public HashMap<Integer, ArrayList<byte[]>> getBytesOfGroup() {
		return bytesOfGroup;
	}

}
