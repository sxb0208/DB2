

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.*;



//All the locking operations takes place in this class.
public class Main {

    //Hash map to store the transaction table ie transaction objects
    public static HashMap<Integer,Transaction> transactionHashMap = new HashMap<Integer,Transaction>();
    //Hash map to store the lock table ie. lock objects
    public static HashMap<String,Lock> lockHashMap = new HashMap<String,Lock>();
    //Used to give timestamp of each transaction
    public static int transactionCount = 0;


    /*
   This method is the entry point of the program. It read the input file line by line and call the respective method for
   further processing.
   @param Input file name from command line
   @throws
   */
    public static void main(String[] args){

        try {
            boolean skipFirstLine = false;
            //Logs all the System.out to the given output file
            PrintStream out = new PrintStream(new FileOutputStream(args[0].split("\\.")[0]+"_output.txt"));
            System.setOut(out);
            // FileReader reads text files.
            FileReader fileReader = new FileReader(args[0]);
            // BufferedReader wraps the filereader for reading the file
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            //this loop is used to read the file line by line
            while((line = bufferedReader.readLine()) != null) {
                if(skipFirstLine){
                    // This prints the input from the file in the console
                    System.out.println("-----------------------------------------------");
                    System.out.println("Line from file :"+line);
                    System.out.println("-----------------------------------------------");
                    //Get the type of operation that has to be taken
                    String operation = getOperationKey(line);
                    //call the respective processing method based on the operation type
                    switch (operation){
                        case "b" :
                            createTransaction(line);
                            break;
                        case "w" :
                            writeOperation(line);
                            break;
                        case "r" :
                            readOperation(line);
                            break;
                        case "e" :
                            commitOperation(line);
                            break;
                    }
                }
                skipFirstLine = true;
            }
            // Always close files.
            bufferedReader.close();
            out.close();
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
    }



    /*
      This method is called to create a transaction object and store it in the transaction data structure
      It loads all the necessary data about the transaction initially.
      @param Input line from the file
      @throws
      */
    public static void createTransaction(String line){

        try{
            String[] createTokens = line.split("");
            //creates transaction object
            Transaction transaction = new  Transaction(Integer.parseInt(createTokens[1]),++transactionCount,TransactionState.ACTIVE);
            //add the transaction object to hashmap table
            transactionHashMap.put(Integer.parseInt(createTokens[1]),transaction);
            System.out.println("Transaction T"+transaction.getTransactionId()+" created and added to the transaction table!");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /*
     This method is called when the write operation is to be performed.
     @param Input line from the file
     @throws
     */
    public static void writeOperation(String line){

        try{
            String[] createTokens = line.split("");
            //Get the current transaction object from the table
            Transaction transactionCurrent = transactionHashMap.get(Integer.parseInt(createTokens[1]));
            //If the transaction is blocked enter this condition and add the value to the blocked operations queue
            if(transactionCurrent.getTransactionState().equals(TransactionState.BLOCKED)){
                PriorityQueue temp=transactionCurrent.getBlockedOperation();
                temp.add(line);
                transactionCurrent.setBlockedOperation(temp);
            }
            //If the transaction is already aborted then no action is taken
            else if(transactionCurrent.getTransactionState().equals(TransactionState.ABORTED)){
                //continue
                System.out.println("Transaction T"+transactionCurrent.getTransactionId()+" is already aborted");
            }
            else {
                //If the data item is not present in the lock table then this condition is entered and the item along with initial value is entered in the lock table
                if (lockHashMap.get(createTokens[4]) == null) {
                    List transIdHoldingLock = new ArrayList();
                    transIdHoldingLock.add(Integer.parseInt(createTokens[1]));
                    Lock lockCurrent = new Lock(createTokens[4],getLockStateMain(createTokens[0]),transIdHoldingLock);
                    lockHashMap.put(createTokens[4],lockCurrent);
                    showLockTable(lockCurrent);
                    List itemsLockedList = transactionCurrent.getItemsLocked();
                    itemsLockedList.add(createTokens[4]);
                    transactionCurrent.setItemsLocked(itemsLockedList);

                }
                //If item is already present in the lock table then this condition is entered
                else {
                    Lock lockCurrent=lockHashMap.get(createTokens[4]);
                    //Check is the lock on the item is RL and only this transaction has the RL lock. If yes then the lock is upgraded to write lock in this condition
                    if(lockCurrent.getLockState()==LockState.RL && lockCurrent.getTransIdHoldingLock().toString().contains(transactionCurrent.getTransactionId()+"") && lockCurrent.getTransIdHoldingLock().size()==1)
                    {
                        lockCurrent.setLockState(LockState.WL);
                        List transIdHoldingLockCurrent = lockCurrent.getTransIdHoldingLock();
                        //check if the transaction already exist if yes then do not add
                        if(!transIdHoldingLockCurrent.toString().contains(Integer.parseInt(createTokens[1])+"")){
                            transIdHoldingLockCurrent.add(Integer.parseInt(createTokens[1]));
                        }
                        lockCurrent.setTransIdHoldingLock(transIdHoldingLockCurrent);
                        List temp = transactionCurrent.getItemsLocked();
                        //check if the item already exist if yes then do not add
                        if(!temp.contains(lockCurrent.getItemName())){
                            temp.add(lockCurrent.getItemName());
                        }
                        transactionCurrent.setItemsLocked(temp);
                    }
                    //If the data item is UL then this condition is entered and the WL lock is given to this transaction and the tables are updated accordingly
                    else if(lockCurrent.getLockState()==LockState.UL)
                    {
                        lockCurrent.setLockState(LockState.WL);
                        List transIdHoldingLockCurrent = lockCurrent.getTransIdHoldingLock();
                        //check if the item already exist if yes then do not add
                        if(!transIdHoldingLockCurrent.toString().contains(Integer.parseInt(createTokens[1])+"")){
                            transIdHoldingLockCurrent.add(Integer.parseInt(createTokens[1]));
                        }
                        lockCurrent.setTransIdHoldingLock(transIdHoldingLockCurrent);
                        List temp = transactionCurrent.getItemsLocked();
                        if(!temp.contains(lockCurrent.getItemName())){
                            temp.add(lockCurrent.getItemName());
                        }
                        transactionCurrent.setItemsLocked(temp);
                    }
                    //If the data item has WL then this condition is entered and the Wait-Die condition is checked
                    else{
                        String cond = checkWaitDie(transactionCurrent.getTransactionTimeStamp(),lockCurrent.getTransIdHoldingLock());
                        //If wait then this transaction's state is changed to blocked and added to the blocked transaction queue
                        if(cond.contains("wait"))
                        {
                            transactionCurrent.setTransactionState(TransactionState.BLOCKED);
                            Transaction blockingTrans=transactionHashMap.get(Integer.parseInt(cond.split("_")[1]));
                            PriorityQueue temp=blockingTrans.getBlockedOperation();
                            temp.add(line);
                            blockingTrans.setBlockedOperation(temp);
                            transactionHashMap.put(blockingTrans.getTransactionId(),blockingTrans);
                            PriorityQueue templw=lockCurrent.getTransIdWaiting();
                            if(!templw.contains(transactionCurrent.getTransactionId())){
                            templw.add(transactionCurrent.getTransactionId());
                            }
                            lockCurrent.setTransIdWaiting(templw);

                        }
                        //If die then this transaction is aborted and all the data item it holds are released
                        else if(cond.contains("die"))
                        {
                            transactionCurrent.setTransactionState(TransactionState.ABORTED);
                            transactionCurrent = release(transactionCurrent);
                            PriorityQueue temp=transactionCurrent.getBlockedOperation();
                            while(!temp.isEmpty())
                            {
                                String eachline=(String)temp.remove();
                                String[] createTokens1 = eachline.split("");
                                Transaction transact = transactionHashMap.get(Integer.parseInt(createTokens1[1]));
                                transact.setTransactionState(TransactionState.ACTIVE);
                                transactionHashMap.put(transact.getTransactionId(),transact);
                                if(eachline.contains("r")){
                                    readOperation(eachline);
                                    executeBlockedtransactions(eachline);
                                }
                                else if(eachline.contains("w")) {
                                    writeOperation(eachline);
                                    executeBlockedtransactions(eachline);
                                }
                                else if(eachline.contains("e"));
                                {
                                    commitOperation(eachline);
                                }
                            }
                        }
                    }
                    lockHashMap.put(lockCurrent.getItemName(),lockCurrent);
                    showLockTable(lockCurrent);         //used show printing the status of the Lock  tables to the file
                }
            }
            transactionHashMap.put(transactionCurrent.getTransactionId(),transactionCurrent);
            showTransactionTable(transactionCurrent);         //used show printing the status of the transaction tables to the file

        }
        catch (Exception ex){
            ex.printStackTrace();
        }

    }

    /*
     This method is called when the read operation is to be performed.
     @param Input line from the file
     @throws
     */
    public static void readOperation(String line){

        try{
            String[] createTokens = line.split("");
            //Get the current transaction object from the table
            Transaction transactionCurrent = transactionHashMap.get(Integer.parseInt(createTokens[1]));
            //If the transaction is blocked enter this condition and add the value to the blocked operations queue
            if(transactionCurrent.getTransactionState().equals(TransactionState.BLOCKED)){
                PriorityQueue temp=transactionCurrent.getBlockedOperation();
                temp.add(line);
                transactionCurrent.setBlockedOperation(temp);
            }
            //If the transaction is already aborted then no action is taken
            else if(transactionCurrent.getTransactionState().equals(TransactionState.ABORTED)){
                //continue
                System.out.println("Transaction T"+transactionCurrent.getTransactionId()+" is already aborted");
            }else {
                //If the data item is not present in the lock table then this condition is entered and the item along with initial value is entered in the lock table
                if (lockHashMap.get(createTokens[4]) == null) {
                    List transIdHoldingLock = new ArrayList();
                    transIdHoldingLock.add(Integer.parseInt(createTokens[1]));
                    Lock lockCurrent = new Lock(createTokens[4],getLockStateMain(createTokens[0]),transIdHoldingLock);
                    lockHashMap.put(createTokens[4],lockCurrent);
                    List itemsLockedList = transactionCurrent.getItemsLocked();
                    if(!itemsLockedList.contains(createTokens[4])){
                        itemsLockedList.add(createTokens[4]);
                    }
                    transactionCurrent.setItemsLocked(itemsLockedList);
                    showLockTable(lockCurrent);
                }
                //If item is already present in the lock table then this condition is entered
                else if(lockHashMap.get(createTokens[4]) != null){
                    Lock lockCurrent = lockHashMap.get(createTokens[4]);
                    //If the data item has read lock then this transaction is also given the RL lock and adds the transaction to the holding lock queue
                    if(lockCurrent.getLockState().equals(LockState.RL) ){
                        List transIdHoldingLockCurrent = lockCurrent.getTransIdHoldingLock();
                        if(!transIdHoldingLockCurrent.toString().contains(Integer.parseInt(createTokens[1])+"")){
                            transIdHoldingLockCurrent.add(Integer.parseInt(createTokens[1]));
                        }
                        lockCurrent.setTransIdHoldingLock(transIdHoldingLockCurrent);
                        List itemLockedCurrent = transactionCurrent.getItemsLocked();
                        if(!itemLockedCurrent.contains(createTokens[4])){
                            itemLockedCurrent.add(createTokens[4]);
                        }
                        transactionCurrent.setItemsLocked(itemLockedCurrent);
                    }
                    //If the data item has WL then Wait-die condition is checked
                    else if(lockCurrent.getLockState().equals(LockState.WL)){
                        String condition = checkWaitDie(transactionCurrent.getTransactionTimeStamp(), lockCurrent.getTransIdHoldingLock());
                        //If wait then this transaction is changed to blocked state and added to blocked transaction queue
                        if(condition.contains("wait")){
                            transactionCurrent.setTransactionState(TransactionState.BLOCKED);
                            Transaction blockingTransactionObj = transactionHashMap.get(condition.split("_")[1]);
                            PriorityQueue blockingOpQueue = blockingTransactionObj.getBlockedOperation();
                            blockingOpQueue.add(line);
                            blockingTransactionObj.setBlockedOperation(blockingOpQueue);
                            transactionHashMap.put(blockingTransactionObj.getTransactionId(),blockingTransactionObj);
                            PriorityQueue templw=lockCurrent.getTransIdWaiting();
                            if(!templw.contains(transactionCurrent.getTransactionId())){
                                templw.add(transactionCurrent.getTransactionId());
                            }
                            lockCurrent.setTransIdWaiting(templw);
                        }
                        //If die then the transaction is aborted and all the data item it holds the lock for is released
                        else if(condition.contains("die")){
                            transactionCurrent.setTransactionState(TransactionState.ABORTED);
                            transactionCurrent = release(transactionCurrent);
                            PriorityQueue temp=transactionCurrent.getBlockedOperation();
                            while(!temp.isEmpty())
                            {
                                String eachline=(String)temp.remove();
                                String[] createTokens1 = eachline.split("");
                                Transaction transact = transactionHashMap.get(Integer.parseInt(createTokens1[1]));
                                transact.setTransactionState(TransactionState.ACTIVE);
                                transactionHashMap.put(transact.getTransactionId(),transact);
                                if(eachline.contains("r")){
                                    readOperation(eachline);
                                    executeBlockedtransactions(eachline);
                                }
                                else if(eachline.contains("w")) {
                                    writeOperation(eachline);
                                    executeBlockedtransactions(eachline);
                                }
                                else if(eachline.contains("e"));
                                {
                                    commitOperation(eachline);
                                }
                            }
                        }
                    }
                    //If unlocked then the RL is given to this transaction and updates both the tables accordingly
                    else if (lockCurrent.getLockState().equals(LockState.UL)){
                        List transIdHoldingLockCurrent = lockCurrent.getTransIdHoldingLock();
                        if(!transIdHoldingLockCurrent.toString().contains(Integer.parseInt(createTokens[1])+"")){
                            transIdHoldingLockCurrent.add(Integer.parseInt(createTokens[1]));
                        }
                        lockCurrent.setTransIdHoldingLock(transIdHoldingLockCurrent);
                        lockCurrent.setLockState(LockState.RL);
                        List itemLockedCurrent = transactionCurrent.getItemsLocked();
                        if(!itemLockedCurrent.contains(createTokens[4])){
                            itemLockedCurrent.add(createTokens[4]);
                        }
                        transactionCurrent.setItemsLocked(itemLockedCurrent);
                    }
                    lockHashMap.put(lockCurrent.getItemName(),lockCurrent);
                    showLockTable(lockCurrent);
                }
            }
            transactionHashMap.put(transactionCurrent.getTransactionId(),transactionCurrent);
            showTransactionTable(transactionCurrent);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /*
     This method is called when a transaction has to be commited.
     @param Input line from the file
     @throws
     */
    public static void commitOperation(String line){
        try{
            String[] createTokens = line.split("");
            //Get the current transaction object from the table
            Transaction transactionCurrent = transactionHashMap.get(Integer.parseInt(createTokens[1]));
            //If the transaction is blocked enter this condition and add the value to the blocked operations queue
            if(transactionCurrent.getTransactionState().equals(TransactionState.BLOCKED)){
                PriorityQueue temp=transactionCurrent.getBlockedOperation();
                temp.add(line);
                transactionCurrent.setBlockedOperation(temp);
            }
            //If the transaction is already aborted then no action is taken
            else if(transactionCurrent.getTransactionState().equals(TransactionState.ABORTED)){
                //continue
                System.out.println("Transaction T"+transactionCurrent.getTransactionId()+" is already aborted");
            }
            //If transaction is ACTIVE then its state is changed to "committed" and all the locks are released and also executing all the operations that was blocked
            //by this transaction in the order of occurrence
            else {
                transactionCurrent.setTransactionState(TransactionState.COMMITTED);
                transactionCurrent = release(transactionCurrent);
                PriorityQueue temp=transactionCurrent.getBlockedOperation();
                while(!temp.isEmpty())
                {
                    String eachline=(String)temp.remove();
                    String[] createTokens1 = eachline.split("");
                    Transaction transact = transactionHashMap.get(Integer.parseInt(createTokens1[1]));
                    transact.setTransactionState(TransactionState.ACTIVE);
                    transactionHashMap.put(transact.getTransactionId(),transact);
                    if(eachline.contains("r")){
                        readOperation(eachline);
                        executeBlockedtransactions(eachline);
                    }
                    else if(eachline.contains("w")) {
                        writeOperation(eachline);
                        executeBlockedtransactions(eachline);
                    }
                    else if(eachline.contains("e"));
                    {
                        commitOperation(eachline);
                    }
                }
                transactionCurrent.setBlockedOperation(temp);
                System.out.println("Transaction T"+transactionCurrent.getTransactionId()+" has been committed");
            }
            transactionHashMap.put(transactionCurrent.getTransactionId(),transactionCurrent);
            showTransactionTable(transactionCurrent);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }

    }

    /*
     This method is used to check for the wait and die condition of the transaction according to which the operations are taken,
     @param currentTransTimeStamp : timestamp of the transaction thats requesting and List of all the transactions that are holding
     the data item.
     @throws
     */
    public static String checkWaitDie(int currentTransTimeStamp ,List holdingTransIDs ){

        String toReturn = null;
        try{
            if(holdingTransIDs.size()>1) {
                Iterator it = holdingTransIDs.iterator();
                while (it.hasNext()) {
                    Transaction transHoldingLock = transactionHashMap.get(it.next());
                    //If the timestamp of current transaction is greater then it is aborted(die)
                    if (currentTransTimeStamp > transHoldingLock.getTransactionTimeStamp()) {
                        return "die";
                    }
                    //If the timestamp of current transaction is lesser then it will be waiting
                    else {
                        toReturn = "wait_" + transHoldingLock.getTransactionId();
                    }
                }
            }
            else
            {
                Transaction transHoldingLock = transactionHashMap.get(holdingTransIDs.get(0));
                //If the timestamp of current transaction is greater then it is aborted(die)
                if (currentTransTimeStamp > transHoldingLock.getTransactionTimeStamp()) {
                    return "die";
                }
                //If the timestamp of current transaction is lesser then it will be waiting
                else {
                    toReturn = "wait_" + transHoldingLock.getTransactionId();
                }
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return toReturn;
    }

    /*
     Method returns appropriate lock value based on the input parameter
     @param Type of the operation in the input line read line by line
     @throws
     */
    public static LockState getLockStateMain(String lockType){
        try{
            if(lockType.equalsIgnoreCase("r")){
                return LockState.RL;
            }else if(lockType.equalsIgnoreCase("w")){
                return LockState.WL;
            }else {
                return LockState.UL;
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }

    /*
     Releases or unlocks all the items that are were locked by this transaction
     @param Transaction ID that needs to release all the data item
     @throws
     */
    public static Transaction release(Transaction currentTrans)
    {
        List ItemsLocked=currentTrans.getItemsLocked();
        int index=0;
        Iterator it = ItemsLocked.iterator();
        //loop through the locked item list and unlock them from this transaction's hold
        while (it.hasNext()){
            Lock currentLock = lockHashMap.get((String) it.next());
            List transIdHoldingLock=currentLock.getTransIdHoldingLock();
            if (currentLock.getLockState() == LockState.WL) {
                currentLock.setLockState(LockState.UL);
                for(int i=0;i<transIdHoldingLock.size();i++)
                {
                    if(transIdHoldingLock.get(i).toString().equals(currentTrans.getTransactionId()+""))
                    {
                        index=i;
                    }
                }
                transIdHoldingLock.remove(index);
                it.remove();
            }
            else if(currentLock.getLockState()==LockState.RL && currentLock.getTransIdHoldingLock().size()>1)
            {
                for(int i=0;i<transIdHoldingLock.size();i++)
                {
                    if(transIdHoldingLock.get(i).toString().equals(currentTrans.getTransactionId()+""))
                    {
                        index=i;
                    }
                }
                transIdHoldingLock.remove(index);
                it.remove();
            }
            else
            {
                currentLock.setLockState(LockState.UL);
                for(int i=0;i<transIdHoldingLock.size();i++)
                {
                    if(transIdHoldingLock.get(i).toString().equals(currentTrans.getTransactionId()+""))
                    {
                        index=i;
                    }
                }
                transIdHoldingLock.remove(index);
                it.remove();
            }
            currentLock.setTransIdHoldingLock(transIdHoldingLock);
            lockHashMap.put(currentLock.getItemName(),currentLock);
        }
        currentTrans.setItemsLocked(ItemsLocked);
        return currentTrans;

    }

    /*
     This method gets the type of operation based on the input line from the file.
     @param Input line from the file
     @throws
     */
    public static String getOperationKey(String line){
        try{
            if(line.contains("b")){
                return "b";
            }else if (line.contains("r")){
                return "r";
            }else if (line.contains("w")){
                return "w";
            }else if (line.contains("e")){
                return "e";
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return "continue";
    }

    /*
     This method executes all the transactions that was blocked by the transaction that's being committed
     @param Operation from the waiting operations queue of committing transaction
     @throws
     */
    public static void executeBlockedtransactions(String line)
    {
        String[] createTokens = line.split("");
        Lock lockCurrent=lockHashMap.get(createTokens[4]);
        PriorityQueue transWaiting=lockCurrent.getTransIdWaiting();
        while(!transWaiting.isEmpty())
        {
            int transIDW=(int)transWaiting.remove();
            Transaction transCurr = transactionHashMap.get(transIDW);
            PriorityQueue temp=transCurr.getBlockedOperation();
            while(!temp.isEmpty())
            {
                String eachline=(String)temp.remove();
                if(eachline.contains("r"))
                    readOperation(eachline);
                if(eachline.contains("w"))
                    writeOperation(eachline);
                if(eachline.contains("e"));
                commitOperation(eachline);
            }
        }
        lockCurrent.setTransIdWaiting(transWaiting);
        lockHashMap.put(lockCurrent.getItemName(),lockCurrent);
    }

    /*
    Prints the transaction table entries of the current transaction
    @param Transaction object of current transaction
    @throws
    */
    public static void showTransactionTable(Transaction currTransaction)
    {
        System.out.println("\n--------------------Transaction Table:----------------------- ");
        System.out.println("Transaction T"+currTransaction.getTransactionId());
        System.out.println("Transaction TimeStamp: "+currTransaction.getTransactionTimeStamp());
        System.out.println("Transaction State: "+currTransaction.getTransactionState());
        System.out.print("Items Locked: ");
        for(int i = 0; i < currTransaction.getItemsLocked().size(); i++) {
            System.out.print(currTransaction.getItemsLocked().get(i)+" ");
        }
        System.out.println("\n");
    }

    /*
   Prints the Lock table entries of specified data item
   @param Lock object of specific data item
   @throws
   */
    public static void showLockTable(Lock currLock)
    {
        System.out.println("\n--------------------Lock Table:----------------------- ");
        System.out.println("Data Item: "+currLock.getItemName());
        System.out.println("Lock Status: "+currLock.getLockState());
        System.out.print("Transactions Holding the lock: ");
        for(int i = 0; i < currLock.getTransIdHoldingLock().size(); i++) {
            System.out.print("T"+currLock.getTransIdHoldingLock().get(i)+" ");
        }
        System.out.println("\n");
    }


}
