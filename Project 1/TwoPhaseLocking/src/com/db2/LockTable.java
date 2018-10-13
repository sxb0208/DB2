package com.db2;

import java.util.*;

public class LockTable
{
	public int transid_WL;
	public List<Integer> transid_RL;	

	public int getTransid_WL() {
		return transid_WL;
	}

	public void setTransid_WL(int transid_WL) {
		this.transid_WL = transid_WL;
	}

	public List<Integer> getTransid_RL() {
		return transid_RL;
	}

	public void setTransid_RL(List<Integer> transid_RL) {
		this.transid_RL = transid_RL;
	}

	//Constructor
	public LockTable()
	{		
		this.transid_WL = 0;
		this.transid_RL = new ArrayList<>();
	}
}