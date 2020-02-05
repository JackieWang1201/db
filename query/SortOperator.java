package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import javax.swing.text.html.HTMLDocument;
import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        if (run == null) {
            return null;
        }
        Iterator<Record> itr = run.iterator();
        ArrayList<Record> input = new ArrayList<>();
        while (itr.hasNext()) {
            input.add(itr.next());
        }
        input.sort(comparator);
        // ArrayBacktrackingIterator<Record> sortedRecords = new ArrayBacktrackingIterator(input.toArray());
        Run sortedRun = createRun();
        sortedRun.addRecords(input);

        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {

        if (runs == null) {
            return null;
        }
        Run mergedRun = createRun();
        if (runs.isEmpty()) {
            return mergedRun;
        }
        PriorityQueue<Pair<Record, Integer>> pq =
                new PriorityQueue<>(runs.size(), new RecordPairComparator());
        ArrayList<Iterator<Record>> itrs = new ArrayList<>(runs.size());
        for (int i = 0; i < runs.size(); i++) {
            itrs.add(runs.get(i).iterator());
            if (itrs.get(i).hasNext()) {
                pq.add(new Pair<>(itrs.get(i).next(), i));

            }
        }
        while (!pq.isEmpty()) {
            Pair<Record, Integer> smaller = pq.remove();
            mergedRun.addRecord(smaller.getFirst().getValues());
            if (itrs.get(smaller.getSecond()).hasNext()) {
                pq.add(new Pair<>(itrs.get(smaller.getSecond()).next(), smaller.getSecond()));

            }
        }

        return mergedRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) {

        if (runs == null) {
            return null;
        }
        if (runs.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        LinkedList<Run> mergedPass = new LinkedList<>();
        for (int i = 0; i < runs.size(); i += numBuffers - 1) {
            Run mergedRun = mergeSortedRuns(runs.subList(i, i + numBuffers - 1));
            mergedPass.add(mergedRun);
        }
        if (runs.size() % (numBuffers - 1) != 0) {
            int floor = Math.floorDiv(runs.size(), numBuffers - 1) * (numBuffers - 1);
            Run mergedRun = mergeSortedRuns(runs.subList(floor, runs.size()));
            mergedPass.add(mergedRun);

        }

        return mergedPass;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {

        Iterator<Page> pageIterator = this.transaction.getPageIterator(tableName);
        BacktrackingIterator<Record> blockIterator;
        List<Run> runs = new ArrayList<>();
        while (pageIterator.hasNext()) {
            blockIterator = this.transaction.getBlockIterator(tableName, pageIterator, numBuffers);
            //InputDataRun inputRun = new InputDataRun(blockIterator);
            Run inputRun = createRunFromIterator(blockIterator);
            Run pass0Run = sortRun(inputRun);
            runs.add(pass0Run);
        }
        while (runs.size() > 1) {
            runs = mergePass(runs);
        }
        return runs.get(0).tableName();
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

