package pe.mrodas.jdbc.helper;

public class TableIterator {

    private final CursorIterator rowIterator;
    private final CursorIterator colIterator;

    public TableIterator() {
        this(0, 0);
    }

    public TableIterator(int totalRows, int totalCols) {
        rowIterator = new CursorIterator(totalRows);
        colIterator = new CursorIterator(totalCols);
    }

    public CursorIterator getRowIterator() {
        return rowIterator;
    }

    public CursorIterator getColIterator() {
        return colIterator;
    }

    public int getTotalRows() {
        return rowIterator.getTotal();
    }

    public int getTotalCols() {
        return colIterator.getTotal();
    }

    public int getPosRow() {
        return rowIterator.getPos();
    }

    public int getPosCol() {
        return colIterator.getPos();
    }

}
