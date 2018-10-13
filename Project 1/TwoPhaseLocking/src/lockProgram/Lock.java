

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;



//Schema of the lock object that is used for storing information about each Data item.
public class Lock {
    public Lock(String itemName, LockState lockState, List transIdHoldingLock) {
        this.itemName = itemName;
        this.lockState = lockState;
        this.transIdHoldingLock = transIdHoldingLock;
    }

    private String itemName;
    private LockState lockState;

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public LockState getLockState() {
        return lockState;
    }

    public void setLockState(LockState lockState) {
        this.lockState = lockState;
    }

    public List getTransIdHoldingLock() {
        return transIdHoldingLock;
    }

    public void setTransIdHoldingLock(List transIdHoldingLock) {
        this.transIdHoldingLock = transIdHoldingLock;
    }

    public PriorityQueue<Integer> getTransIdWaiting() {
        return transIdWaiting;
    }

    public void setTransIdWaiting(PriorityQueue<Integer> transIdWaiting) {
        this.transIdWaiting = transIdWaiting;
    }

    private List transIdHoldingLock = new ArrayList();
    private PriorityQueue<Integer> transIdWaiting = new PriorityQueue<Integer>();


    public static void main(String[] args){
        List transIdHoldingLock = new ArrayList();
        transIdHoldingLock.add(1);
        transIdHoldingLock.add(2);
         int index =  transIdHoldingLock.indexOf(1);
        transIdHoldingLock.remove(index);
        System.out.println("testing : "+ transIdHoldingLock.size());

    }

}
