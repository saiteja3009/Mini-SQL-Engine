#include "global.h"

bool syntacticParseNESTEDJOIN(){
    logger.log("syntactic parse nested join");

    if(tokenizedQuery.size()!=13 || tokenizedQuery[2]!="JOIN" || tokenizedQuery[3]!="USING" || tokenizedQuery[4]!="NESTED" || tokenizedQuery[11]!="BUFFER" || tokenizedQuery[7]!="ON"){
        cout<<"Syntax error"<<endl;
        return false;
    }
    
    for(auto it:tokenizedQuery[12]){
        if(it<'0' || it>'9'){
            cout<<"Syntax error : Invalid Buffer size"<<endl;
            return false;
        }
    }

    if(tokenizedQuery[9]!="==" && tokenizedQuery[9]!="!=" && tokenizedQuery[9]!="<" && tokenizedQuery[9]!="<=" && tokenizedQuery[9]!="=<" && tokenizedQuery[9]!=">" && tokenizedQuery[9]!=">=" && tokenizedQuery[9]!="=>"){
        cout<<"Syntax error : Invalid binary operator"<<endl;
        return false;
    }


    parsedQuery.queryType=NESTEDJOIN;
    parsedQuery.nestedJoinResult=tokenizedQuery[0];
    parsedQuery.nestedJoinTableName1=tokenizedQuery[5];
    parsedQuery.nestedJoinTableName2=tokenizedQuery[6];
    parsedQuery.nestedAttribute1=tokenizedQuery[8];
    parsedQuery.nestedBinaryOperation=tokenizedQuery[9];
    parsedQuery.nestedAttribute2=tokenizedQuery[10];
    parsedQuery.bufferSize=stoi(tokenizedQuery[12]);
    

    return true;


}


bool semanticParseNESTEDJOIN() {
    logger.log("syntactic parse nested join");

    if(tableCatalogue.isTable(parsedQuery.nestedJoinTableName1)==false || tableCatalogue.isTable(parsedQuery.nestedJoinTableName2)==false){
        cout << "Semantic error : Table doesn't exist" << endl;
        return false;
    }

    if(tableCatalogue.isTable(parsedQuery.nestedJoinResult)){
        cout << "Semantic error : Result table already exists" << endl;
        return false;
    }

    if(tableCatalogue.isColumnFromTable(parsedQuery.nestedAttribute1, parsedQuery.nestedJoinTableName1)==false || tableCatalogue.isColumnFromTable(parsedQuery.nestedAttribute2, parsedQuery.nestedJoinTableName2)==false){
        cout << "Semantic error : Column doesn't exist" << endl;
        return false;
    }
    if(parsedQuery.bufferSize<3){
        cout<<"Semantic error : Number of buffers are less than 3, so nested join is not possible";
        return false;
    }
    return true;
}

bool checkNestedCondition(int a,int b,string c){
    if(c=="=="){
        return a==b;
    }
    if(c==">=" || c=="=>"){
        return a>=b;
    }
    if(c=="<=" || c=="=<"){
        return a<=b;
    }
    if(c=="!="){
        return a!=b;
    }
    if(c==">"){
        return a>b;
    }
    if(c=="<"){
        return a<b;
    }
    return false;

}


void executeNESTEDJOIN(){
    logger.log("Execute Nested Join");
    
    Table tableA=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName1));
    Table tableB=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName2));

    Cursor cursorA=tableA.getCursor();
    Cursor cursorB=tableB.getCursor();

    int attrA=tableA.getColumnIndex(parsedQuery.nestedAttribute1);
    int attrB=tableB.getColumnIndex(parsedQuery.nestedAttribute2);

    int pageA=tableA.maxRowsPerBlock;
    int pageB=tableB.maxRowsPerBlock;

    vector<string> columns;
    int total_buffer_size=(parsedQuery.bufferSize-2)*pageA;
    int total_block_access=0;

    for(int i=0;i<tableA.columnCount;i++){
        columns.push_back(tableA.columns[i]);
    }

    for(int i=0;i<tableB.columnCount;i++){
        columns.push_back(tableB.columns[i]);
    }

    // Table *resultTable = new Table(parsedQuery.nestedJoinResult,columns);
    Table* resultTable = new Table(parsedQuery.nestedJoinResult);
    vector<string> result_table_columns = tableA.columns;
    result_table_columns.insert(result_table_columns.end(),tableB.columns.begin(),tableB.columns.end());

    resultTable->columns = result_table_columns;
    // resultantTable->rowCount = totalRows;
    resultTable->columnCount = result_table_columns.size();
    int maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * resultTable->columnCount));
    resultTable->maxRowsPerBlock = maxRowsPerBlock;
    vector<vector<int>> rowsInPage;
    int pageCounter=0;

    vector<int> rowsA=cursorA.getNext();
    vector<int> rowsB;
    vector<vector<int>> temp_firsttable;


    // while(rowsA.empty()==false){
    while(true){
        if(rowsA.empty())
            break;
        temp_firsttable.clear();
        int cur=0;
        while(rowsA.empty()==false && temp_firsttable.size()!=total_buffer_size){
            temp_firsttable.push_back(rowsA);
            cur+=1;
            if(cur==pageA){
                total_block_access+=1;
                cur=0;
            }
            rowsA=cursorA.getNext();
        }
        if(cur!=0){
            cur=0;
            total_block_access+=1;
        }
        cursorB=tableB.getCursor();
        rowsB=cursorB.getNext();
        while(rowsB.empty()==false){
            vector<vector<int>> secondtable;
            while(rowsB.empty()==false && secondtable.size()!=pageB){
                secondtable.push_back(rowsB);
                rowsB=cursorB.getNext();
            }
            total_block_access+=1;
            for(auto a: temp_firsttable){
                for(auto b: secondtable){
                    if(checkNestedCondition(a[attrA],b[attrB],parsedQuery.nestedBinaryOperation)){
                        vector<int> cur_temp=a;
                        cur_temp.insert(cur_temp.end(),b.begin(),b.end());
                        rowsInPage.push_back(cur_temp);
                        if(rowsInPage.size()==maxRowsPerBlock)
                        {
                            bufferManager.writePage(parsedQuery.nestedJoinResult, pageCounter, rowsInPage, maxRowsPerBlock);
                            resultTable->rowCount+=maxRowsPerBlock;
                            rowsInPage.clear();
                            pageCounter++;
                            resultTable->rowsPerBlockCount.push_back(maxRowsPerBlock);
                            total_block_access++;
                        }
                    }
                }
            }
        }

    }
    if(rowsInPage.size()>0)
    {
        // cout<<" hello";
        bufferManager.writePage(parsedQuery.nestedJoinResult, pageCounter, rowsInPage, rowsInPage.size());
        resultTable->rowCount+=rowsInPage.size();
        pageCounter++;
        resultTable->rowsPerBlockCount.push_back(rowsInPage.size());
        rowsInPage.clear();
        total_block_access++;
    }
    resultTable->blockCount=pageCounter;
    tableCatalogue.insertTable(resultTable);
    cout<<"Total count of block_accesses are : "<< total_block_access<<endl;
    return;
}