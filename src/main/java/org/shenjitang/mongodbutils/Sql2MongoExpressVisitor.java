/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.shenjitang.mongodbutils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;
import java.lang.reflect.Method;
import java.util.Date;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
/**
 *
 * @author xiaolie
 */
public class Sql2MongoExpressVisitor extends ExpressionVisitorAdapter {
    private Bson query = null;
    private BsonDocument currentObj;
//    private String currentColumn;

    public Sql2MongoExpressVisitor() {
    }

    public Bson getQuery() {
        return query;
    }

    public void setQuery(BsonDocument query) {
        this.query = query;
    }
    
    private void addQuery(Bson expreBson) {
        if (query == null) {
            query = expreBson;
        } else {
            query = Filters.and(query, expreBson);
        }
    }

    @Override
    public void visit(EqualsTo expr) {  
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        addQuery(Filters.eq(left.toString(), toBsonValue(right))); 
        visitBinaryExpression(expr);
    }
    

    @Override
    public void visit(GreaterThan expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        addQuery(Filters.gt(left.toString(), toBsonValue(right)));        
        visitBinaryExpression(expr);
    }

    @Override
    public void visit(Between expr) {
        Expression left = expr.getLeftExpression();
        Expression start = expr.getBetweenExpressionStart();
        Expression end = expr.getBetweenExpressionEnd();
        addQuery(Filters.gte(left.toString(), toBsonValue(start)));
        addQuery(Filters.lte(left.toString(), toBsonValue(end)));
    }
    
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Expression left = greaterThanEquals.getLeftExpression();
        Expression right = greaterThanEquals.getRightExpression();
        addQuery(Filters.gte(left.toString(), toBsonValue(right))); 
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        addQuery(Filters.lt(left.toString(), toBsonValue(right))); 
        visitBinaryExpression(expr);
    }
    
    @Override
    public void visit(MinorThanEquals expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        addQuery(Filters.lte(left.toString(), toBsonValue(right))); 
        visitBinaryExpression(expr);
    }
    
    @Override
    public void visit(InExpression expr) {
        Expression left = expr.getLeftExpression();
        ItemsList list = expr.getRightItemsList();
        String listStr = list.toString();
        String str = listStr.substring(listStr.indexOf("(") + 1, listStr.lastIndexOf(")"));
        String[] items = str.split(",");
        trimQuotation(items);
        addQuery(Filters.in(left.toString(), items));
    }

    private void trimQuotation(String[] items) {
        for (int i = 0; i < items.length; i++) {
            String item = items[i].trim();
            if (item.startsWith("'") && item.endsWith("'")) {
                item = item.substring(1, item.length() - 1);
                items[i] = item;
            }
        }
    }
    
    
    private BsonValue toBsonValue(Object value) {
        BsonValue bv = null;
        if (value instanceof Expression) {
            try {
                Method getValueMethod = value.getClass().getMethod("getValue");
                value = getValueMethod.invoke(value);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        if (value instanceof Integer) {
            bv = new BsonInt32((Integer) value);
        } else if (value instanceof Long) {
            bv = new BsonInt64((Long) value);
        } else if (value instanceof Date) {
            bv = new BsonDateTime(((Date) value).getTime());
        } else if (value instanceof Double) {
            bv = new BsonDouble((Double) value);
        } else if (value instanceof Boolean) {
            bv = new BsonBoolean((Boolean) value);
        } else {
            bv = new BsonString(value.toString());
        }
        return bv;
    }

}
