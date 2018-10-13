

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;



//Schema of the Transaction object that is used for storing information about each Transaction.
public class Transaction {

    private int transactionId;
    private int transactionTimeStamp;
    private TransactionState transactionState;
    private List itemsLocked = new ArrayList();
    private PriorityQueue<String> blockedOperation = new PriorityQueue<String>();

    public Transaction(int transactionId, int transactionTimeStamp, TransactionState transactionState) {
        this.transactionId = transactionId;
        this.transactionTimeStamp = transactionTimeStamp;
        this.transactionState = transactionState;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public int getTransactionTimeStamp() {
        return transactionTimeStamp;
    }

    public void setTransactionTimeStamp(int transactionTimeStamp) {
        this.transactionTimeStamp = transactionTimeStamp;
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }

    public void setTransactionState(TransactionState transactionState) {
        this.transactionState = transactionState;
    }

    public List getItemsLocked() {
        return itemsLocked;
    }

    public void setItemsLocked(List itemsLocked) {
        this.itemsLocked = itemsLocked;
    }

    public PriorityQueue<String> getBlockedOperation() {
        return blockedOperation;
    }

    public void setBlockedOperation(PriorityQueue<String> blockedOperation) {
        this.blockedOperation = blockedOperation;
    }
}
