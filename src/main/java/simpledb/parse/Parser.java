package simpledb.parse;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Parser {
    private Lexer lex;

    public Parser(String s) {
        lex = new Lexer(s);
    }

    public String field() {
        return lex.eatId();
    }

    public Constant constant() {
        if (lex.matchStringConstant())
            return new Constant(lex.eatStringConstant());
        else
            return new Constant(lex.eatIntConstant());
    }

    public Expression expression() {
        if (lex.matchId())
            return new Expression(field());
        else
            return new Expression(constant());
    }

    public Term term() {
        Expression lhs = expression();
        lex.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() {
        Predicate pred = new Predicate(term());
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    public QueryData query() {
        lex.eatKeyword("select");
        List<String> fields = selectList();
        lex.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new QueryData(fields, tables, pred);
    }

    private List<String> selectList() {
        List<String> L = new ArrayList<>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(selectList());
        }
        return L;
    }

    private Collection<String> tableList() {
        Collection<String> L = new ArrayList<>();
        L.add(lex.eatId());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(tableList());
        }
        return L;
    }

    public Object updateCmd() {
        if (lex.matchKeyword("insert"))
            return insert();
        else if (lex.matchKeyword("delete"))
            return delete();
        else if (lex.matchKeyword("update"))
            return modify();
        else
            return create();
    }

    private Object create() {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table"))
            return createTable();
        else if (lex.matchKeyword("view"))
            return createView();
        else
            return createIndex();
    }

    private CreateIndexData createIndex() {
        lex.eatKeyword("index");
        String idxName = lex.eatId();
        lex.eatKeyword("on");
        String tblName = lex.eatId();
        lex.eatDelim('(');
        String fieldName = field();
        lex.eatDelim(')');
        return new CreateIndexData(idxName, tblName, fieldName);
    }

    private CreateViewData createView() {
        lex.eatKeyword("view");
        String viewName = lex.eatId();
        lex.eatKeyword("as");
        QueryData qd = query();
        return new CreateViewData(viewName, qd);
    }

    private CreateTableData createTable() {
        lex.eatKeyword("table");
        String tblName = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblName, sch);
    }

    private Schema fieldDefs() {
        Schema sch = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema sch2 = fieldDefs();
            sch.addAll(sch2);
        }
        return sch;
    }

    private Schema fieldDef() {
        String fieldName = field();
        return fieldType(fieldName);
    }

    private Schema fieldType(String fieldName) {
        Schema sch = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            sch.addIntField(fieldName);
        } else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            sch.addStringField(fieldName, strLen);
        }
        return sch;
    }

    private DeleteData delete() {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblName = lex.eatId();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblName, pred);
    }

    private InsertData insert() {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblName = lex.eatId();
        lex.eatDelim('(');
        List<String> fields = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblName, fields, vals);
    }

    private List<String> fieldList() {
        List<String> L = new ArrayList<>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private List<Constant> constList() {
        List<Constant> L = new ArrayList<>();
        L.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }
        return L;
    }


    private ModifyData modify() {
        lex.eatKeyword("update");
        String tblName = lex.eatId();
        lex.eatKeyword("set");
        String fieldName = field();
        lex.eatDelim('=');
        Expression newVal = expression();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblName, fieldName, newVal, pred);
    }


}
