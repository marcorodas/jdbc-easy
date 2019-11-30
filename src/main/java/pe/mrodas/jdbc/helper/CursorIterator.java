package pe.mrodas.jdbc.helper;

import java.util.Iterator;

public class CursorIterator implements Iterator<Integer>, Iterable<Integer> {

    private final int total;
    private int cursor = 0;

    public CursorIterator(int total) {
        this.total = total;
    }

    @Override
    public boolean hasNext() {
        return cursor < total;
    }

    @Override
    public Integer next() {
        return cursor++;
    }

    public int getTotal() {
        return total;
    }

    public int getPos() {
        return cursor == 0 ? 0 : cursor - 1 ;
    }

    public CursorIterator reset() {
        cursor = 0;
        return this;
    }

    @Override
    public Iterator<Integer> iterator() {
        return this;
    }
}
