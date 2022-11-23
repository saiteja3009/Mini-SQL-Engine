#include "global.h"

bool syntacticParseGROUPBY(){
    logger.log("Syntactic Parse Group By");
    if(tokenizedQuery.size() != 9 || tokenizedQuery[2]!="GROUP" || tokenizedQuery[3]!="BY" || tokenizedQuery[5]!="FROM" || tokenizedQuery[7]!="RETURN"){
        cout<<"Syntax Error"<<endl;
        return false;
    }

    parsedQuery.aggFunction = tokenizedQuery[8].substr(0,3);
    parsedQuery.aggColumn=tokenizedQuery[8].substr(4);
    parsedQuery.aggColumn.pop_back();
    

    if(parsedQuery.aggFunction!="AVG" && parsedQuery.aggFunction!="MAX" && parsedQuery.aggFunction!="MIN" && parsedQuery.aggFunction!="SUM"){
        cout<<"Syntax Error"<<endl;
        return false;
    }

    parsedQuery.queryType=GROUPBY;
    parsedQuery.groupByAttributeName=tokenizedQuery[4];
    parsedQuery.sourceTableName=tokenizedQuery[6];
    parsedQuery.groupByTableName=tokenizedQuery[0];
    
    return true;
}


bool semanticParseGROUPBY(){
    logger.log("Semantic Parse Group By");

    if(tableCatalogue.isTable(parsedQuery.sourceTableName)==false){
        cout<<"Semantic error : Table does not exist"<<endl;
        return false;
    }

    if(tableCatalogue.isTable(parsedQuery.groupByTableName)){
        cout<<"Semantic error : Table already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.aggColumn,parsedQuery.sourceTableName) || (!tableCatalogue.isColumnFromTable(parsedQuery.groupByAttributeName,parsedQuery.sourceTableName))){
        cout<<"Semantic error : Column does not exist"<<endl;
        return false;
    }

    return true;

}

void executeGROUPBY(){
    logger.log("Execute Group By");

    Table table=*(tableCatalogue.getTable(parsedQuery.sourceTableName));
    Cursor cursor=table.getCursor();

    vector<string> table_columns;
    table_columns.push_back(parsedQuery.groupByAttributeName);
    table_columns.push_back(parsedQuery.aggFunction+parsedQuery.aggColumn);

    Table *result_table= new Table(parsedQuery.groupByTableName,table_columns);

    int groupbyindex,attrindex;

    attrindex=table.getColumnIndex(parsedQuery.aggColumn);
    groupbyindex=table.getColumnIndex(parsedQuery.groupByAttributeName);

    if(parsedQuery.aggFunction == "SUM"){
        vector<int> rows=cursor.getNext();
        unordered_map<int,int>mp;
        while(rows.empty()==false){
            if(mp.find(rows[groupbyindex])==mp.end()){
                mp[rows[groupbyindex]]=rows[attrindex];
            }
            else{
                mp[rows[groupbyindex]]+=rows[attrindex];
            }
            rows=cursor.getNext();
        }
        stack<vector<int>> st;
        for(auto i:mp){
            vector<int> temp;
            temp.push_back(i.first);
            temp.push_back(i.second);
            st.push(temp);
            }
        while(!st.empty()){
            vector<int>temp=st.top();
            st.pop();
            result_table->writeRow<int>(temp);
        }
    }


    if(parsedQuery.aggFunction == "MIN"){
        vector<int> rows=cursor.getNext();
        unordered_map<int,int>mp;
        while(rows.empty()==false){
            if(mp.find(rows[groupbyindex])==mp.end()){
                mp[rows[groupbyindex]]=rows[attrindex];
            }
            else{
                mp[rows[groupbyindex]]=min(mp[rows[groupbyindex]],rows[attrindex]);
            }
            rows=cursor.getNext();
        }
        stack<vector<int>> st;
        for(auto i:mp){
            vector<int> temp;
            temp.push_back(i.first);
            temp.push_back(i.second);
            st.push(temp);
            }
        while(!st.empty()){
            vector<int>temp=st.top();
            st.pop();
            result_table->writeRow<int>(temp);
        }
        }

    if(parsedQuery.aggFunction == "MAX"){
        vector<int> rows=cursor.getNext();
        unordered_map<int,int>mp;
        while(rows.empty()==false){
            if(mp.find(rows[groupbyindex])==mp.end()){
                mp[rows[groupbyindex]]=rows[attrindex];
            }
            else{
                mp[rows[groupbyindex]]=max(mp[rows[groupbyindex]],rows[attrindex]);
            }
            rows=cursor.getNext();
        }
        stack<vector<int>> st;
        for(auto i:mp){
            vector<int> temp;
            temp.push_back(i.first);
            temp.push_back(i.second);
            st.push(temp);
            }
        while(!st.empty()){
            vector<int>temp=st.top();
            st.pop();
            result_table->writeRow<int>(temp);
        }
        }
    
    if(parsedQuery.aggFunction == "AVG"){
        vector<int> rows=cursor.getNext();
        unordered_map<int,int>mp;
        unordered_map<int,int> counter;
        while(rows.empty()==false){
            if(mp.find(rows[groupbyindex])==mp.end()){
                mp[rows[groupbyindex]]=rows[attrindex];
                counter[rows[groupbyindex]]=1;
            }
            else{
                mp[rows[groupbyindex]]+=rows[attrindex];
                counter[rows[groupbyindex]]++;
            }
            rows=cursor.getNext();
        }
        stack<vector<int>> st;
        for(auto i:mp){
            vector<int> temp;
            temp.push_back(i.first);
            float avg=(i.second*1.0)/counter[i.first];
            // cout<<i.second<<" "<<counter[i.first]<<" "<<i.first<<" "<<avg<<"\n";
            int x=round(avg);
            temp.push_back(avg);
            st.push(temp);
            }
        while(!st.empty()){
            vector<int>temp=st.top();
            st.pop();
            result_table->writeRow<int>(temp);
        }
        }
        
    
    result_table->blockify();
    tableCatalogue.insertTable(result_table);
    cout<<"Total number of block accesses : "<< table.blockCount + result_table->blockCount<<endl;
    return;

}