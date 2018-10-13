package com.db2;

import java.util.*;

public class TransactionProcess {

	private String[] filedata = new String[100];

	public void readTransactions() {
		for (int i = 0; i < TransStart.readData.length; i++) {
			filedata[i] = TransStart.readData[i];
		}
		int i = 0;
		while (filedata[i] != null) {
			System.out.println("Operation: " + filedata[i]);
			switch (filedata[i].substring(0, 1)) {
			/*
			 * Beginning of a new transaction
			 */
			case "b":
				Transaction transaction = new Transaction("Active");
				int tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf(";")));
				TransStart.transMap.put(tid, transaction);
				System.out.println("Begin Transaction: T" + tid);
				break;
			/*
			 * Read operation for a transaction. A data item can have one or
			 * more read operations. Read-Read is not a conflicting transaction.
			 * Lock not given is same data item is write-locked by some other
			 * transaction.
			 */
			case "r":
				tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf("(")));
				if (TransStart.transMap.get(tid).getTrans_state() != "Aborted") {
					LockTable L = new LockTable();
					List<Integer> readList = null;
					List<String> lockItemList = null;
					String itemname = filedata[i].substring(filedata[i].indexOf("(") + 1, filedata[i].indexOf(")"));

					// check if exists in locktable- yes:check read/write lock
					// No:insert into locktable

					if (!TransStart.lockMap.containsKey(itemname)) {
						readList = new ArrayList<Integer>();
						lockItemList = new ArrayList<String>();
						readList.add(tid);
						L.setTransid_RL(readList);
						TransStart.lockMap.put(itemname, L);
						if (TransStart.transMap.get(tid).getItems_locked() != null) {
							lockItemList = TransStart.transMap.get(tid).getItems_locked();
						}
						lockItemList.add(itemname);
						TransStart.transMap.get(tid).setItems_locked(lockItemList);
						System.out.println("T" + tid + " has a read lock on item " + itemname);
					} else {
						for (String key : TransStart.lockMap.keySet()) {

							if (key.equals(itemname)) {
								if (TransStart.lockMap.get(itemname).getTransid_RL() != null) {
									List<Integer> existingReadList = TransStart.lockMap.get(itemname).getTransid_RL();
									existingReadList.add(tid);
									TransStart.lockMap.get(itemname).setTransid_RL(existingReadList);
									lockItemList = TransStart.transMap.get(tid).getItems_locked();
									lockItemList.add(itemname);
									TransStart.transMap.get(tid).setItems_locked(lockItemList);
									System.out.println("T" + tid + " has a read lock on item " + itemname);
								}

								else if ((TransStart.lockMap.get(itemname).getTransid_WL()) != 0) {
									System.out.println("Item " + key + " is Writelocked and not available!");
									check_deadlock(tid, itemname, "r");
								} else {
									readList = new ArrayList<Integer>();
									readList.add(tid);
									TransStart.lockMap.get(itemname).setTransid_RL(readList);
									if (TransStart.transMap.get(tid).getItems_locked() != null) {
										lockItemList = TransStart.transMap.get(tid).getItems_locked();
									}
									lockItemList.add(itemname);
									TransStart.transMap.get(tid).setItems_locked(lockItemList);
									System.out.println("T" + tid + " has a read lock on item " + itemname);
								}
							}
						}
					}
				} else
					System.out.println("Operation r" + tid + " could not be performed as transaction " + tid
							+ " is already aborted!");
				break;
			/*
			 * Write operation of a transaction.A data item can have only one
			 * write operation. Lock not given is same data item is write-locked
			 * by some other transaction. Read lock can be upgraded to write
			 * lock is read lock exist by same transaction of same data item
			 */
			case "w":
				tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf("(")));
				if (TransStart.transMap.get(tid).getTrans_state() != "Aborted") {
					List<String> lockItemList = null;
					LockTable L1 = new LockTable();
					String itemname1 = filedata[i].substring(filedata[i].indexOf("(") + 1, filedata[i].indexOf(")"));
					// check if exists in locktable- yes:check read/write lock
					// No:insert into locktable
					if (!TransStart.lockMap.containsKey(itemname1)) {
						lockItemList = new ArrayList<String>();
						L1.setTransid_WL(tid);
						TransStart.lockMap.put(itemname1, L1);
						if (TransStart.transMap.get(tid).getItems_locked() != null) {
							lockItemList = TransStart.transMap.get(tid).getItems_locked();
						}
						lockItemList.add(itemname1);
						TransStart.transMap.get(tid).setItems_locked(lockItemList);
						System.out.println("T" + tid + " has a write lock on item " + itemname1);
					} else {
						for (String key : TransStart.lockMap.keySet()) {
							if (key.equals(itemname1)) {
								if ((TransStart.lockMap.get(itemname1).getTransid_WL() != 0)) {
									System.out.println("Item " + key
											+ " is Writelocked and not available! Proccessing for wait and die condition");
									// Check for wait and die condition
									check_deadlock(tid, itemname1, "w");
								}
								List<Integer> readList1 = TransStart.lockMap.get(itemname1).getTransid_RL();
								if ((readList1.size() == 1) && readList1.get(0) == tid) {
									upgradeReadToWrite(tid, itemname1);
								} else {
									// check wait and die here again put it in a
									// waiting list . so once all
									// the read list item unlock it. It will get
									// the lock
									check_deadlock(tid, itemname1, "w");
								}
							}
						}
					}
				} else
					System.out.println("Operation w" + tid + " could not be performed as transaction " + tid
							+ " is already aborted!");
				break;
			/*
			 * End of a transaction. Status of the transaction will be committed
			 * if active and all data items held by that data item will be
			 * released.
			 */
			case "e":
				tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf(";")));
				checkEndTransaction(tid);
				break;
			}
			i++;
		}
	}

	/**
	 * Checking end condition for transaction when operation end_transaction is
	 * executed and then executing unlock transaction function.
	 */
	public void checkEndTransaction(int tid) {

		if (TransStart.transMap.get(tid).getTrans_state() != "Aborted") {
			if (TransStart.waitTransactionid.contains(tid) && !TransStart.waitTransactionList.contains("e" + tid)) {
				TransStart.waitTransactionList.add("e" + tid);
				System.out.println("Other Operations for transaction " + tid + " are in waiting. So, Operation e" + tid
						+ " has been added to waitlist and could not be committed.");
			} else {
				TransStart.transMap.get(tid).setTrans_state("Committed");
				System.out
						.println("Transaction " + tid + " has committed and released all the locks held on data items");
				unlockTransaction(tid);
				TransStart.waitTransactionList.remove("e" + tid);
				TransStart.waitTransactionid.remove(tid);
			}
		} else
			System.out.println(
					"Operation e" + tid + " could not be performed as transaction " + tid + " is already aborted!");
	}

	/**
	 * This function upgrades read lock on a data item by a transaction to write
	 * lock.
	 */
	private void upgradeReadToWrite(int tid, String itemname1) {
		List<Integer> readList1;
		readList1 = null;
		TransStart.lockMap.get(itemname1).setTransid_RL(readList1);
		// making read list empty before upgrading
		// the lock
		TransStart.lockMap.get(itemname1).setTransid_WL(tid);
		System.out.println("T" + tid + " has upgraded to write lock from read Lock on item " + itemname1);
	}

	/**
	 * This function releases locks held by transaction and assign the next
	 * applicable transaction in the waiting list and transferring the locks to
	 * the new transaction.
	 */
	public void unlockTransaction(Integer tid) {
		if (!TransStart.transMap.get(tid).getItems_locked().isEmpty()) {
			List<String> lockedItemList = TransStart.transMap.get(tid).getItems_locked();
			/**
			 * Loop to iterate over item and see if they have any other lock if
			 * they have lock on different transaction id then remove the lock
			 * of the transaction which is either aborting or committing
			 */
			if (lockedItemList != null) {
				for (String itemName : lockedItemList) {
					if (TransStart.lockMap.get(itemName).getTransid_RL() != null) {
						List<Integer> transidRL = TransStart.lockMap.get(itemName).getTransid_RL();
						Iterator<Integer> iterator = transidRL.iterator();
						while (iterator.hasNext()) {
							Integer commitAbortTid = iterator.next();
							if (commitAbortTid == tid) {
								iterator.remove();
							}
						}
						TransStart.lockMap.get(itemName).setTransid_RL(transidRL);
					} else {
						if (TransStart.lockMap.get(itemName).getTransid_WL() != 0) {
							Integer transidWL = TransStart.lockMap.get(itemName).getTransid_WL();
							if (transidWL == tid) {
								transidWL = 0;
							}
							TransStart.lockMap.get(itemName).setTransid_WL(transidWL);
						}
					}
					// REFINING WAIT LIST
					// if (!TransStart.waitTransactionList.isEmpty()) {
					// int i = 0;
					// for (String waitTransaction :
					// TransStart.waitTransactionList) {
					// Integer removedTransaction =
					// Integer.parseInt(waitTransaction.substring(1, 2));
					// if (removedTransaction == tid) {
					// TransStart.waitTransactionList.remove(i);
					// }
					// i++;
					// }
					// Iterator<String> iterator =
					// TransStart.waitTransactionList.iterator();
					// while (iterator.hasNext()) {
					// Integer commitAbortTid =
					// Integer.parseInt(iterator.next().substring(1, 2));
					// if (commitAbortTid == tid) {
					// iterator.remove();
					// }
					// }
					// }
					String waitingTransaction = null;
					int index = -1;
					if (!TransStart.waitTransactionList.isEmpty()) {
						for (String waitTransaction : TransStart.waitTransactionList) {
							if (!waitTransaction.substring(0, 1).equals("e")) {
								if (waitTransaction.substring(2, 3).equals(itemName)) {
									waitingTransaction = waitTransaction;
									index++;
									break;
								}
							}
						}
						// waitingTransaction =
						// TransStart.waitTransactionList.get(0);
						if (waitingTransaction != null || index != -1) {
							Integer readWaitListId = Integer.parseInt(waitingTransaction.substring(1, 2));
							String itemName1 = waitingTransaction.substring(2, 3);
							Integer writeLockTid = TransStart.lockMap.get(itemName).getTransid_WL();
							List<Integer> readList1 = null;
							readList1 = TransStart.lockMap.get(itemName).getTransid_RL();
							if (waitingTransaction.substring(0, 1).equals("w")) {
								if (readList1 != null) {
									if (readList1.size() == 1 && readList1.contains(readWaitListId)
											&& itemName1.equals(itemName)) {
										upgradeReadToWrite(readWaitListId, itemName1);
									} else {
										System.out.println("Transaction " + waitingTransaction.substring(0, 2)
												+ " keeps waiting in the wait list");
									}
								}
								if ((readList1 == null || writeLockTid == 0) && itemName1.equals(itemName)) {
									System.out.println(
											"Processing the operation w" + readWaitListId + " from the wait list.");
									writeLockTid = readWaitListId;
									TransStart.lockMap.get(itemName).setTransid_WL(writeLockTid);
									// TransStart.waitTransactionList.remove(0);
									TransStart.waitTransactionList.remove(index);
									System.out.println("Assigning lock to operation w" + writeLockTid
											+ " from waiting list on item " + itemName1);
									for (String waitTransaction : TransStart.waitTransactionList) {
										if (Integer.parseInt(waitTransaction.substring(1, 2)) == writeLockTid
												&& waitTransaction.substring(0, 1).equals("e")) {
											checkEndTransaction(writeLockTid);
										}
									}
								}
								if (readWaitListId != writeLockTid) {
									System.out.println("Transaction " + waitingTransaction.substring(0, 2)
											+ " keeps waiting in the wait list");
								}
							}

							else if (waitingTransaction.substring(0, 1).equals("r")) {
								if (writeLockTid == 0 && itemName1.equals(itemName)) {

									if (readList1 == null) {
										readList1 = new ArrayList<Integer>();
									}
									readList1.add(readWaitListId);
									TransStart.lockMap.get(itemName).setTransid_RL(readList1);
									// TransStart.waitTransactionList.remove(0);
									TransStart.waitTransactionList.remove(index);
									System.out.println("Assigning lock to operation r" + writeLockTid
											+ " from waiting list on item " + itemName1);
									for (String waitTransaction : TransStart.waitTransactionList) {
										if (Integer.parseInt(waitTransaction.substring(1, 2)) == writeLockTid
												&& waitTransaction.substring(0, 1).equals("e")) {
											checkEndTransaction(writeLockTid);
										}
									}
								} else {
									System.out.println("Transaction " + waitingTransaction.substring(0, 2)
											+ " keeps waiting in the wait list");
								}
							}
						}
					}
				}
			}
			// making locked item list empty of transaction committed or aborted
			lockedItemList = null;
			TransStart.transMap.get(tid).setItems_locked(lockedItemList);
		}
	}

	/**
	 * The function checks deadlock between conflicting operations by applying
	 * wait and die method.
	 */
	private void check_deadlock(int tid, String itemname1, String oper) {
		int timestamp_requesting_trans = TransStart.transMap.get(tid).getTrans_timestamp();
		int transid_itemHolding_trans = 0, timestamp_itemHolding_trans = 0;
		List<Integer> read_transid = TransStart.lockMap.get(itemname1).getTransid_RL();
		// check if item is readlocked or writelocked by a transaction and
		// retrieve that transaction id
		if (TransStart.lockMap.get(itemname1).getTransid_WL() != 0) {
			transid_itemHolding_trans = TransStart.lockMap.get(itemname1).getTransid_WL();
		} else {
			transid_itemHolding_trans = Collections.min(read_transid);
		}
		timestamp_itemHolding_trans = TransStart.transMap.get(transid_itemHolding_trans).getTrans_timestamp();
		if (timestamp_requesting_trans <= timestamp_itemHolding_trans) {
			TransStart.transMap.get(tid).setTrans_state("Blocked");
			if (transid_itemHolding_trans == tid && oper.equals("w")) {
				TransStart.waitTransactionList.addLast(oper + tid + itemname1);
				TransStart.waitTransactionid.add(tid);
			} else if (TransStart.lockMap.get(itemname1).getTransid_WL() == tid) {
				TransStart.waitTransactionList.addLast(oper + tid + itemname1);
				TransStart.waitTransactionid.add(tid);
			}
			System.out.println(oper + tid + " is waiting for item " + itemname1);
		} else {
			TransStart.transMap.get(tid).setTrans_state("Aborted");
			System.out.println("Transaction T" + tid + " is aborted");
			unlockTransaction(tid);
		}
		return;
	}
}
