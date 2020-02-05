package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Stack;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        if (lockContext.getEffectiveLockType(transaction) == lockType) {
            return;
        }
        if (lockType == LockType.NL) { //?
            return;
        }

        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }

        // auto-escalate
        if (ifAutoEscalate(transaction, lockContext)) {
            LockContext tableContext = lockContext.parentContext();
            tableContext.escalate(transaction);
        }
        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;

        }

        ensureLocksOnAncestors(lockContext, lockType);

        if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else if (LockType.substitutable(lockType, lockContext.getExplicitLockType(transaction))) {
            lockContext.promote(transaction, lockType);
        } else if ((lockContext.getExplicitLockType(transaction) == LockType.IX
                && lockType == LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
        } else {
            lockContext.escalate(transaction);
            if (lockContext.getExplicitLockType(transaction) == LockType.S
                    && lockType == LockType.X) {
                lockContext.promote(transaction, lockType);

            }
        }

        return;
    }


   public static void ensureLocksOnAncestors(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        LockContext pCtx = lockContext.parentContext();
        Stack<LockContext> path = new Stack<>();
        if (pCtx == null || LockType.canBeParentLock(pCtx.getExplicitLockType(transaction), lockType)) {
            return;
        } else {
            path.push(pCtx);
            pCtx = pCtx.parentContext();
        }
        if (lockType == LockType.S) {
            while (pCtx != null
                    && !LockType.canBeParentLock(pCtx.getExplicitLockType(transaction), LockType.IS)) {
                path.push(pCtx);
                pCtx = pCtx.parentContext();
            }

        } else {
            while (pCtx != null
                    && !LockType.canBeParentLock(pCtx.getExplicitLockType(transaction), LockType.IX)) {
                path.push(pCtx);
                pCtx = pCtx.parentContext();
            }

        }

        while (!path.isEmpty()) {
            LockContext ctx = path.pop();
            if (lockType == LockType.S) {
                if (ctx.getExplicitLockType(transaction) == LockType.NL) {
                    ctx.acquire(transaction, LockType.IS);
                } else {
                    //if (LockType.substitutable())
                    ctx.promote(transaction, LockType.IS);

                }
            } else {
                if (ctx.getExplicitLockType(transaction) == LockType.NL) {
                    ctx.acquire(transaction, LockType.IX);
                } else {
                    if (ctx.getExplicitLockType(transaction) == LockType.S) {
                        ctx.promote(transaction, LockType.SIX);
                    } else {
                        ctx.promote(transaction, LockType.IX);

                    }

                }
            }
        }

    }


    private static boolean ifAutoEscalate(TransactionContext transaction, LockContext lockContext) {
        if (!isPage(lockContext)) {
            return false;
        }
        LockContext tableContext = lockContext.parentContext();
        if (!tableContext.ifEscalateEnabled()) {
            return false;
        }
        double saturation = tableContext.saturation(transaction);
        double numTablePages = tableContext.capacity();

        return saturation >= 0.2 && numTablePages >= 10;
    }

    private static boolean isPage(LockContext lockContext) {
        LockContext pctx = lockContext.parentContext();
        int depth = 0;
        while (pctx != null) {
            pctx = pctx.parentContext();
            depth++;
        }
        return depth == 2;
    }

    public static void ensureLocksAbovePage(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        LockContext pCtx = lockContext;
        Stack<LockContext> path = new Stack<>();
        if (pCtx == null || LockType.canBeParentLock(pCtx.getExplicitLockType(transaction), lockType)) {
            return;
        } else {
            path.push(pCtx);
            pCtx = pCtx.parentContext();
        }
        if (lockType == LockType.S) {
            while (pCtx != null
                    && !LockType.canBeParentLock(pCtx.getExplicitLockType(transaction), LockType.IS)) {
                path.push(pCtx);
                pCtx = pCtx.parentContext();
            }

        } else {
            while (pCtx != null
                    && !LockType.canBeParentLock(pCtx.getExplicitLockType(transaction), LockType.IX)) {
                path.push(pCtx);
                pCtx = pCtx.parentContext();
            }

        }

        while (!path.isEmpty()) {
            LockContext ctx = path.pop();
            if (lockType == LockType.S) {
                if (ctx.getExplicitLockType(transaction) == LockType.NL) {
                    ctx.acquire(transaction, LockType.IS);
                } else {
                    //if (LockType.substitutable())
                    ctx.promote(transaction, LockType.IS);

                }
            } else {
                if (ctx.getExplicitLockType(transaction) == LockType.NL) {
                    ctx.acquire(transaction, LockType.IX);
                } else {
                    if (ctx.getExplicitLockType(transaction) == LockType.S) {
                        ctx.promote(transaction, LockType.SIX);
                    } else {
                        ctx.promote(transaction, LockType.IX);

                    }

                }
            }
        }

    }

}
