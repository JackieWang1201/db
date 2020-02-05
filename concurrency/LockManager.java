package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        public void processQueue() {
            boolean process = true;
            for (int i = 0; i < waitingQueue.size(); i++) {
                LockRequest request = waitingQueue.peek();
                for (Lock lock : locks) {
                    if (!LockType.compatible(lock.lockType, request.lock.lockType)) {
                        if (request.releasedLocks.contains(lock)) {
                            continue;
                        }
                        process = false;
                        break;
                    }
                }
                if (process) {
                    waitingQueue.pop();
                    for (Lock lock : request.releasedLocks) {
                        release(request.transaction, lock.name); //?
                    }
                    locks.add(request.lock);
                    addTransactionLock(request.transaction.getTransNum(), request.lock);
                    request.transaction.unblock();
                } else {
                    break;
                }
            }

        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    public void addTransactionLock(Long transactionNum, Lock lock) {
        if (transactionLocks.get(transactionNum) == null) {
            transactionLocks.put(transactionNum, new ArrayList<>());
        }
        transactionLocks.get(transactionNum).add(lock);
    }

    public void rmTransactionLock(Long transactionNum, Lock lock) {
        if (transactionLocks.get(transactionNum) == null) {
            transactionLocks.put(transactionNum, new ArrayList<>());
        }
        if (!transactionLocks.get(transactionNum).contains(lock)) {
            throw new NoLockHeldException("no lock held by" + transactionNum.toString());
        }
        transactionLocks.get(transactionNum).remove(lock);
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            boolean block = false;
            boolean substitute = false;
            int count = 0;
            List<Lock> transactionLocks = getLocks(transaction);
            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            Lock substituded = null; // just initialize
            for (Lock lock : getLocks(name)) {
                if (lock.transactionNum == transaction.getTransNum()) {
                    count++;
                    if (count > 1) {
                        throw new DuplicateLockRequestException("duplicate lock by" + transaction.toString());
                    }
//                    if (LockType.substitutable(lockType, lock.lockType)) {
//                        substitute = true;
//                        substituded = lock;
//                    } else {
//                        throw new DuplicateLockRequestException("duplicate lock by" + transaction.toString());
//                    }
                    if (releaseLocks.contains(lock.name)) {
                        substitute = true;
                        substituded = lock;
                    } else {
                        throw new DuplicateLockRequestException("duplicate lock by" + transaction.toString());
                    }

                }

                if (lock.transactionNum != transaction.getTransNum()
                        && !LockType.compatible(lock.lockType, lockType)) {
                    entry.waitingQueue.addFirst(new LockRequest(transaction, newLock));
                    transaction.prepareBlock();
                    block = true;
                    break;
                }
            }
            if (!block) {
                if (substitute) {
                    substituded.lockType = lockType;

                    boolean found = false;
                    for (Lock lock : transactionLocks) {
                        if (lock.name.equals(name)) {
                            found = true;
                            lock.lockType = lockType;
                            break;
                        }
                    }
                    if (!found) {
                        throw new NoLockHeldException("no lock held by" + transaction.toString());
                    }

                    entry.processQueue();

                } else {
                    entry.locks.add(newLock);
                    //transactionLocks.add(newLock);
                    addTransactionLock(transaction.getTransNum(), newLock);


                }
                // modify resourceEntry
                // release locks
                for (ResourceName rName : releaseLocks) {
                    ResourceEntry rEntry = getResourceEntry(rName);
                    List<Lock> locks = rEntry.locks;
                    //Queue<LockRequest> waitingQueue = rEntry.waitingQueue;
                    boolean found = false;
                    if (rName.equals(name) && substitute) {
                        continue;
                    }
                    for (Lock lock: locks) {
                        if (lock.transactionNum == transaction.getTransNum()) {
                            found = true;
                            locks.remove(lock);
                            rEntry.processQueue();
                            break;
                        }
                    }
                    if (!found) {
                        throw new NoLockHeldException("no lock held by" + transaction.toString());
                    }
                }
                // process waiting queue



                // release locks in transactionLocks
                for (ResourceName rName : releaseLocks) {
                    boolean found = false;
                    for (Lock lock : transactionLocks) {
                        if (lock.name.equals(rName)) {
                            found = true;
                            if (rName.equals(name) && substitute) {
                                continue;
                            } else {
//                                transactionLocks.remove(lock);
                                rmTransactionLock(transaction.getTransNum(), lock);
                            }
                            break;
                        }
                    }
                    if (!found) {
                        throw new NoLockHeldException("no lock held by" + transaction.toString());
                    }
                }
                return;
            }
        }
        // block
        transaction.block();
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            boolean block = false;
            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            if (!entry.waitingQueue.isEmpty()) {
                entry.waitingQueue.addLast(new LockRequest(transaction, newLock)); // released??
                transaction.prepareBlock();
                block = true;
            }
            for (Lock lock : getLocks(name)) {
                if (block) {
                    break;
                }
                if (lock.transactionNum == transaction.getTransNum()) {
                    throw new DuplicateLockRequestException("duplicate lock by" + transaction.toString());
                }

                if (!LockType.compatible(lock.lockType, lockType)) {
                    entry.waitingQueue.addLast(new LockRequest(transaction, newLock)); // released??
                    transaction.prepareBlock();
                    block = true;
                    break;
                }
            }
            if (!block) {
                entry.locks.add(newLock);
                addTransactionLock(transaction.getTransNum(), newLock);
                return;
            }
        }
        // block
        transaction.block();
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            List<Lock> transactionLocks = getLocks(transaction);
            ResourceEntry entry = getResourceEntry(name);
            boolean found = false;
            for (Lock lock : transactionLocks) {
                if (lock.name.equals(name)) {
                    found = true;
                    rmTransactionLock(transaction.getTransNum(), lock);
                    break;
                }
            }
            if (!found) {
                throw new NoLockHeldException("no lock held by" + transaction.toString());
            }
            found = false;
            for (Lock lock : getLocks(name)) {
                if (lock.transactionNum == transaction.getTransNum()) {
                    found = true;
                    entry.locks.remove(lock);
                    entry.processQueue();
                }
            }
            if (!found) {
                throw new NoLockHeldException("no lock held by" + transaction.toString());
            }


            return;
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        synchronized (this) {
            boolean block = false;
            List<Lock> transactionLocks = getLocks(transaction);
            ResourceEntry entry = getResourceEntry(name);
            boolean found = false;
            for (Lock lock : transactionLocks) {
                if (lock.name.equals(name)) {
                    found = true;
                    if (lock.lockType == newLockType) {
                        throw new DuplicateLockRequestException("invalid lock held by" + transaction.toString());
                    }
                    if (!LockType.substitutable(newLockType, lock.lockType) || lock.lockType == newLockType) {
                        throw new InvalidLockException("invalid lock held by" + transaction.toString());
                    }
                    break;
                }
            }
            if (!found) {
                throw new NoLockHeldException("no lock held by" + transaction.toString());
            }

            // update
            Lock lockToUpdate = null;
            Lock newLock = null;
            for (Lock lock : getLocks(name)) {
                if (lock.transactionNum == transaction.getTransNum()) {
                    lockToUpdate = lock;
                    newLock = new Lock(name, newLockType, lock.transactionNum);
                    break;
                }
            }
            for (Lock lock : getLocks(name)) {
                if (lock.transactionNum != transaction.getTransNum()
                        && !LockType.compatible(lock.lockType, newLockType)) {
                    List<Lock> releasedLocks = new ArrayList<>();
                    releasedLocks.add(lockToUpdate);
                    entry.waitingQueue.addFirst(new LockRequest(transaction, newLock, releasedLocks));
                    transaction.prepareBlock();
                    block = true;
                    break;
                }
            }
            if (!block) {
                lockToUpdate.lockType = newLockType;

                for (Lock lock : transactionLocks) {
                    if (lock.name.equals(name)) {
                        lock.lockType = newLockType;
                        break;
                    }
                }
                // entry.processQueue();
                return;
            }
        }
        // block
        transaction.block();
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        //
        for (Lock lock : getLocks(name)) {
            if (lock.transactionNum == transaction.getTransNum()) {
                return lock.lockType;
            }
        }

        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
