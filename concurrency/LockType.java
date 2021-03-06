package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (a == LockType.NL || b == LockType.NL) {
            return true;
        } else if (a == LockType.S) {
            if (b == LockType.IS || b == LockType.S) {
                return true;
            }
        } else if (a == LockType.IS) {
            if (b == LockType.IS || b == LockType.IX || b == LockType.S || b == LockType.SIX) {
                return true;
            }
        } else if (a == LockType.IX) {
            if (b == LockType.IS || b == LockType.IX) {
                return true;
            }
        } else if (a == LockType.SIX) {
            if (b == LockType.IS) {
                return true;
            }
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
//        LockType pLock = parentLock(childLockType);
//        return compatible(parentLockType, pLock);

        if (childLockType == NL) {
            return true;
        } else if (parentLockType == NL) {
            return false;
        } else if (parentLockType == IS) {
            if (childLockType == IS || childLockType == S) {
                return true;
            }
        } else if (parentLockType == IX) {
            return true;
        } else if (parentLockType == SIX) {
            if (childLockType == IX || childLockType == X) {
                return true;
            }
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if (required == NL) {
            return true;
        } else if (substitute == NL) {
            return false;
        } else if (required == S) {
            if (substitute == S || substitute == SIX || substitute == X) {
                return true;
            }
        } else if (required == X) {
            if (substitute == X) {
                return true;
            }
        } else if (required == IS) {
            if (substitute == IS || substitute == IX) {
                return true;
            }
        } else if (required == IX) {
            if (substitute == IX) {
                return true;
            }
        } else if (required == SIX) {
            if (substitute == SIX) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

