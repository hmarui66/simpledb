package simpledb.parse;

public class CreateViewData {
    private String viewName;
    private QueryData qd;

    public CreateViewData(String viewName, QueryData qd) {
        this.viewName = viewName;
        this.qd = qd;
    }

    public String viewName() {
        return viewName;
    }

    public String viewDef() {
        return qd.toString();
    }
}
