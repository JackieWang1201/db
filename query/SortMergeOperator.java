package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            SortOperator left = new SortOperator(SortMergeOperator.this.getTransaction(),
                    this.getLeftTableName(), new LeftRecordComparator());
            SortOperator right = new SortOperator(SortMergeOperator.this.getTransaction(),
                    this.getRightTableName(), new RightRecordComparator());
            String sortedLeftTable = left.sort();
            String sortedRightTable = right.sort();
            this.leftIterator = SortMergeOperator.this.getRecordIterator(sortedLeftTable);
            this.rightIterator = SortMergeOperator.this.getRecordIterator(sortedRightTable);

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            // We mark the first record so we can reset to it when we advance the left record.
//            if (rightRecord != null) {
//                rightIterator.markPrev();
//            } else { return; }
            marked = false;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }


//        /**
//         * After this method is called, rightRecord will contain the first record in the rightSource.
//         * There is always a first record. If there were no first records (empty rightSource)
//         * then the code would not have made it this far. See line 69.
//         */
//        private void resetRightRecord() {
//            this.rightIterator.reset();
//            assert(rightIterator.hasNext());
//            rightRecord = rightIterator.next();
//        }
//
//        /**
//         * Advances the left record
//         *
//         * The thrown exception means we're done: there is no next record
//         * It causes this.fetchNextRecord (the caller) to hand control to its caller.
//         */
//        private void nextLeftRecord() {
//            if (!leftIterator.hasNext()) { throw new NoSuchElementException("All Done!"); }
//            leftRecord = leftIterator.next();
//        }

        /**
         * Pre-fetches what will be the next record, and puts it in this.nextRecord.
         * Pre-fetching simplifies the logic of this.hasNext() and this.next()
         */
        private void fetchNextRecord() {
            DataBox leftJoinValue;
            DataBox rightJoinValue;
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            this.nextRecord = null;
            do {
                if (leftRecord == null) {
                    throw new NoSuchElementException("No new record to fetch");
                }
                if (!marked) {
                    if (leftRecord == null) {
                        throw new NoSuchElementException("No new record to fetch");
                    }
                    leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

                    while(leftJoinValue.compareTo(rightJoinValue) < 0) {
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                        if (leftRecord == null) {
                            throw new NoSuchElementException("No new record to fetch");
                        }
                        leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    }
                    while(leftJoinValue.compareTo(rightJoinValue) > 0) {
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                        if (rightRecord == null) {
                            break;
                        }
                        rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    }
                    if (rightRecord != null) {
                        marked = true;
                        rightIterator.markPrev();

                    }

                }
                leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                rightJoinValue = rightRecord != null
                        ? rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()) : null;
                if (rightRecord != null && leftJoinValue.equals(rightJoinValue)) {
                    List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    nextRecord = new Record(leftValues);
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                } else {
                    rightIterator.reset();
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    marked = false;
                }
            } while (!hasNext());
        }




        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
            //throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
