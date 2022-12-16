package simpledb.parse;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

public class Lexer {
    private Collection<String> keywords;
    private StreamTokenizer tok;

    public Lexer(String s) {
        initKeywords();
        tok = new StreamTokenizer(new StringReader(s));
        tok.ordinaryChar('.'); // disallow "." in identifiers
        tok.wordChars('_', '_'); // allow "_" in identifiers
        tok.lowerCaseMode(true); // ids and keywords are converted
        nextToken();
    }

    public boolean matchDelim(char d) {
        return d == (char)tok.ttype;
    }

    public boolean matchIntConstant() {
        return tok.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchStringConstant() {
        return '\'' == tok.ttype;
    }

    public boolean matchKeyword(String w) {
        return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
    }

    public boolean matchId() {
        return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
    }

    /**
     * Throw an exception if the current token is not the
     * specified delimiter.
     * Otherwise, moves to the next token.
     */
    public void eatDelim(char d) {
        if (!matchDelim(d))
            throw new BadSyntaxException();
        nextToken();
    }

    public int eatIntConstant() {
        if (!matchIntConstant())
            throw new BadSyntaxException();
        int i = (int) tok.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() {
        if (!matchStringConstant())
            throw new BadSyntaxException();
        String s = tok.sval;
        nextToken();
        return s;
    }

    public void eatKeyword(String w) {
        if (!matchKeyword(w))
            throw new BadSyntaxException();
        nextToken();
    }

    public String eatId() {
        if (!matchId())
            throw new BadSyntaxException();
        String s = tok.sval;
        nextToken();
        return s;
    }


    private void nextToken() {
        try {
            tok.nextToken();
        } catch (IOException e) {
            throw new BadSyntaxException(e);
        }
    }

    private void initKeywords() {
        keywords = Arrays.asList("select", "from", "where", "and", "insert", "into", "values", "delete", "update",
                "set", "create", "table", "int", "varchar", "view", "as", "index", "on");
    }

}
