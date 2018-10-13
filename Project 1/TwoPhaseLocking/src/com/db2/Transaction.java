package com.db2;

import java.util.*;

public class Transaction {
	public static int TS = 0;
	public int trans_timestamp;
	public String trans_state;
	public List<String> items_locked;
	
	public void setItems_locked(List<String> items_locked) {
		this.items_locked = items_locked;
	}

	public Transaction() {
	}

	public Transaction(String trans_state) {
		this.trans_state = trans_state;
		this.items_locked = new ArrayList<String>();
		this.trans_timestamp = ++TS;
	}
	
	public List<String> getItems_locked() {
		return this.items_locked;
	}

	public String getTrans_state() {

		return this.trans_state;
	}

	public int getTrans_timestamp() {

		return this.trans_timestamp;
	}

	public void setTrans_state(String state) {

		this.trans_state = state;
	}
}