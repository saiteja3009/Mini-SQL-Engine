#include "global.h"

bool syntacticParsePARTHASHJOIN(){
    logger.log("Syntactic parse part hash join");

    if(tokenizedQuery.size()!=13 || tokenizedQuery[2]!="JOIN" || tokenizedQuery[3]!="USING" || tokenizedQuery[4]!="PARTHASH" || tokenizedQuery[11]!="BUFFER" || tokenizedQuery[7]!="ON"){
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
    
    parsedQuery.queryType = PARTHASH;
    parsedQuery.nestedJoinResult = tokenizedQuery[0];
    parsedQuery.nestedJoinTableName1 = tokenizedQuery[5];
    parsedQuery.nestedJoinTableName2 = tokenizedQuery[6];
    parsedQuery.nestedAttribute1 = tokenizedQuery[8];
    parsedQuery.nestedBinaryOperation = tokenizedQuery[9];
    parsedQuery.nestedAttribute2 = tokenizedQuery[10];
    parsedQuery.bufferSize = stoi(tokenizedQuery[12]);
    
    return true;

}


bool semanticParsePARTHASHJOIN() {
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
        cout<<"Semantic error : Number of buffers are less than 3, so part hash join is not possible";
        return false;
    }

    return true;
}

bool checkNestedConditionPart(int a,int b,string c){
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


Table* join(int no_of_tables,int &total_block_access){

    vector<string> columns;
    Table tableA=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName1));
    Table tableB=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName2));

    // Cursor cursorA=tableA.getCursor();
    // Cursor cursorB=tableB.getCursor();

    int attrA=tableA.getColumnIndex(parsedQuery.nestedAttribute1);
    int attrB=tableB.getColumnIndex(parsedQuery.nestedAttribute2);
    // int total_block_access=0;

    for(int i=0;i<tableA.columnCount;i++){
        columns.push_back(tableA.columns[i]);
    }

    for(int i=0;i<tableB.columnCount;i++){
        columns.push_back(tableB.columns[i]);
    }

    // cout<<tableA.tableName<<" "<<tableB.tableName<<" "<<attrA<<" "<<attrB<<"\n";
    Table *resultTable = new Table(parsedQuery.nestedJoinResult,columns);
    // Table* resultTable = new Table(parsedQuery.nestedJoinResult);
    // vector<string> result_table_columns = tableA.columns;
    // result_table_columns.insert(result_table_columns.end(),tableB.columns.begin(),tableB.columns.end());

    // resultTable->columns = result_table_columns;
    // // resultantTable->rowCount = totalRows;
    // resultTable->columnCount = result_table_columns.size();
    // int maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * resultTable->columnCount));
    // resultTable->maxRowsPerBlock = maxRowsPerBlock;
    // vector<vector<int>> rowsInPage;
    // int pageCounter=0;

    for(int i=0;i<no_of_tables;i++){

        Table tableA=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName1+"_PART"+to_string(i)));
        Table tableB=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName2+"_PART"+to_string(i)));

        if(tableA.rowCount>0 && tableB.rowCount>0){
            // total_block_access+=tableA.blockCount * tableB.blockCount;
            Cursor cursorA=tableA.getCursor();
            Cursor cursorB=tableB.getCursor();

            // int attrA=tableA.getColumnIndex(parsedQuery.nestedAttribute1);
            // int attrB=tableB.getColumnIndex(parsedQuery.nestedAttribute2);

            int pageA=tableA.maxRowsPerBlock;
            int pageB=tableB.maxRowsPerBlock;

            int total_buffer_size=(parsedQuery.bufferSize-2)*pageA;

            vector<int> rowsA=cursorA.getNext();
            vector<int> rowsB;
            vector<vector<int>> temp_firsttable;


            // while(rowsA.empty()==false)
            while(true){
                if(rowsA.empty())
                    break;
                temp_firsttable.clear();
                int cur=0;
                while(rowsA.empty()==false && temp_firsttable.size()!=total_buffer_size){
                    temp_firsttable.push_back(rowsA);
                    rowsA=cursorA.getNext();
                    cur+=1;
                    if(cur==pageA){
                        cur=0;
                        total_block_access+=1;
                    }
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
                            if(checkNestedConditionPart(a[attrA],b[attrB],parsedQuery.nestedBinaryOperation)){
                                vector<int> cur_temp;
                                cur_temp=a;
                                cur_temp.insert(cur_temp.end(),b.begin(),b.end());
                                resultTable->writeRow<int>(cur_temp);
                            }
                        }
                    }
                }

            }
        }
    }
    return resultTable;
}


void executePARTHASHJOIN(){
    logger.log("Execute part hash join");
    Table tableA=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName1));
    Table tableB=*(tableCatalogue.getTable(parsedQuery.nestedJoinTableName2));

    Cursor cursorA=tableA.getCursor();
    Cursor cursorB=tableB.getCursor();

    int attrA=tableA.getColumnIndex(parsedQuery.nestedAttribute1);
    int attrB=tableB.getColumnIndex(parsedQuery.nestedAttribute2);

    



    int total_block_access=0;

    int buffers=parsedQuery.bufferSize-1;

    vector<string> tableAcolumns;
    for(int i=0;i<tableA.columnCount;i++){
        tableAcolumns.push_back(tableA.columns[i]);
    }
    vector<Table*> tableABuffers(buffers);
    // cout<<buffers<<"wdwd";
    for(int i=0;i<buffers;i++){
        tableABuffers[i]=new Table(tableA.tableName+"_PART"+to_string(i),tableAcolumns);
    }
    
    // cursorA=tableA.getCursor();
    vector<int>rowsA=cursorA.getNext();

    while(rowsA.empty()==false){
        int hash=rowsA[attrA]%buffers;
        // cout<<hash<<" ";
        tableABuffers[hash]->writeRow<int>(rowsA);
        rowsA=cursorA.getNext();
    }
    total_block_access+=tableA.blockCount;
    for(int i=0;i<buffers;i++){
        tableABuffers[i]->blockify();
        total_block_access+=tableABuffers[i]->blockCount;
        tableCatalogue.insertTable(tableABuffers[i]);
    }

    vector<string> tableBcolumns;
    for(int i=0;i<tableB.columnCount;i++){
        tableBcolumns.push_back(tableB.columns[i]);
    }
    vector<Table*> tableBBuffers(buffers);
    for(int i=0;i<buffers;i++){
        tableBBuffers[i]=new Table(tableB.tableName+"_PART"+to_string(i),tableBcolumns);
    }
    cout<<endl;
    
    // cursorB=tableB.getCursor();
    vector<int>rowsB=cursorB.getNext();
    cout<<tableA.columnCount<<" "<<tableB.columnCount<<"\n";
    while(rowsB.empty()==false){
        int hash=rowsB[attrB]%buffers;
        // cout<<hash<<" "<<rowsB[attrB]<<"\n";
        tableBBuffers[hash]->writeRow<int>(rowsB);
        rowsB=cursorB.getNext();
    }
    total_block_access+=tableB.blockCount;
    for(int i=0;i<buffers;i++){
        tableBBuffers[i]->blockify();
        total_block_access+=tableBBuffers[i]->blockCount;
        tableCatalogue.insertTable(tableBBuffers[i]);
    }



    Table* resultTable = join(parsedQuery.bufferSize-1,total_block_access);
    resultTable->blockify();
    tableCatalogue.insertTable(resultTable);
    for(int i=0;i<parsedQuery.bufferSize-1;i++){
        tableCatalogue.deleteTable(parsedQuery.nestedJoinTableName1+"_PART"+to_string(i));
        tableCatalogue.deleteTable(parsedQuery.nestedJoinTableName2+"_PART"+to_string(i));
    }
    cout<<tableB.blockCount<<" "<<tableA.blockCount<<" "<<total_block_access<<" "<<resultTable->blockCount<<"\n";
    cout<<"Total no of block accesses : "<<total_block_access+resultTable->blockCount<<endl;
    return;

}





// #include "global.h"

// bool syntacticParsePARTHASHJOIN(){
//     logger.log("Syntactic parse part hash join");

//     if(tokenizedQuery.size()!=13 || tokenizedQuery[2]!="JOIN" || tokenizedQuery[3]!="USING" || tokenizedQuery[4]!="PARTHASH" || tokenizedQuery[11]!="BUFFER" || tokenizedQuery[7]!="ON"){
//         cout<<"Syntax error"<<endl;
//         return false;
//     }

//     for(auto it:tokenizedQuery)
//     cout<<it<<" ";
//     cout<<endl;
    
//     for(auto it:tokenizedQuery[12]){
//         if(it<'0' || it>'9'){
//             cout<<"Syntax error : Invalid Buffer size"<<endl;
//             return false;
//         }
//     }

//     if(tokenizedQuery[9]!="==" && tokenizedQuery[9]!="!=" && tokenizedQuery[9]!="<" && tokenizedQuery[9]!="<=" && tokenizedQuery[9]!="=<" && tokenizedQuery[9]!=">" && tokenizedQuery[9]!=">=" && tokenizedQuery[9]!="=>"){
//         cout<<"Syntax error : Invalid binary operator"<<endl;
//         return false;
//     }
    
//     parsedQuery.queryType = PARTHASH;
//     parsedQuery.partHashJoinResult = tokenizedQuery[0];
//     parsedQuery.partHashJoinTableName1 = tokenizedQuery[5];
//     parsedQuery.partHashJoinTableName2 = tokenizedQuery[6];
//     parsedQuery.partHashAttribute1 = tokenizedQuery[8];
//     parsedQuery.partHashAttribute2 = tokenizedQuery[10];
//     parsedQuery.partHashBufferSize = stoi(tokenizedQuery[12]);
//     parsedQuery.partHashBinaryOperation = tokenizedQuery[9];
    
//     return true;

// }


// bool semanticParsePARTHASHJOIN() {
//     logger.log("syntactic parse nested join");

//     if(tableCatalogue.isTable(parsedQuery.partHashJoinTableName1)==false || tableCatalogue.isTable(parsedQuery.partHashJoinTableName2)==false){
//         cout << "Semantic error : Table doesn't exist" << endl;
//         return false;
//     }

//     if(tableCatalogue.isTable(parsedQuery.partHashJoinResult)){
//         cout << "Semantic error : Result table already exists" << endl;
//         return false;
//     }

//     if(tableCatalogue.isColumnFromTable(parsedQuery.partHashAttribute1, parsedQuery.partHashJoinTableName1)==false || tableCatalogue.isColumnFromTable(parsedQuery.partHashAttribute2, parsedQuery.partHashJoinTableName2)==false){
//         cout << "Semantic error : Column doesn't exist" << endl;
//         return false;
//     }

//     if(parsedQuery.bufferSize<3){
//         cout<<"Semantic error : Number of buffers are less than 3, so part hash join is not possible";
//         return false;
//     }

//     return true;
// }

// bool checkNestedConditionPart(int a,int b,string c){
//     if(c=="=="){
//         return a==b;
//     }
//     if(c==">=" || c=="=>"){
//         return a>=b;
//     }
//     if(c=="<=" || c=="=<"){
//         return a<=b;
//     }
//     if(c=="!="){
//         return a!=b;
//     }
//     if(c==">"){
//         return a>b;
//     }
//     if(c=="<"){
//         return a<b;
//     }
//     return false;

// }


// Table* join(int no_of_tables,int &total_block_access){

//     vector<string> columns;
//     Table tableA=*(tableCatalogue.getTable(parsedQuery.partHashJoinTableName1));
//     Table tableB=*(tableCatalogue.getTable(parsedQuery.partHashJoinTableName2));

//     // Cursor cursorA=tableA.getCursor();
//     // Cursor cursorB=tableB.getCursor();

//     int attrA=tableA.getColumnIndex(parsedQuery.partHashAttribute1);
//     int attrB=tableB.getColumnIndex(parsedQuery.partHashAttribute2);
//     // int total_block_access=0;

//     for(int i=0;i<tableA.columnCount;i++){
//         columns.push_back(tableA.columns[i]);
//     }

//     for(int i=0;i<tableB.columnCount;i++){
//         columns.push_back(tableB.columns[i]);
//     }

//     Table *resultTable = new Table(parsedQuery.partHashJoinResult,columns);

//     for(int i=0;i<no_of_tables;i++){

//         Table tableA=*(tableCatalogue.getTable(parsedQuery.partHashJoinTableName1+"_"+to_string(i)));
//         Table tableB=*(tableCatalogue.getTable(parsedQuery.partHashJoinTableName2+"_"+to_string(i)));

//         if(tableA.rowCount>0 && tableB.rowCount>0){
//             total_block_access+=tableA.blockCount * tableB.blockCount;
//             Cursor cursorA=tableA.getCursor();
//             Cursor cursorB=tableB.getCursor();

//             // int attrA=tableA.getColumnIndex(parsedQuery.nestedAttribute1);
//             // int attrB=tableB.getColumnIndex(parsedQuery.nestedAttribute2);

//             int pageA=tableA.maxRowsPerBlock;
//             int pageB=tableB.maxRowsPerBlock;

//             int total_buffer_size=(parsedQuery.bufferSize-2)*pageA;

//             vector<int> rowsA=cursorA.getNext();
//             vector<int> rowsB;
//             vector<vector<int>> temp_firsttable;


//             while(rowsA.empty()==false){
//                 temp_firsttable.clear();
//                 int cur=0;
//                 while(rowsA.empty()==false && temp_firsttable.size()!=total_buffer_size){
//                     temp_firsttable.push_back(rowsA);
//                     rowsA=cursorA.getNext();
//                 }
//                 cursorB=tableB.getCursor();
//                 rowsB=cursorB.getNext();
//                 while(rowsB.empty()==false){
//                     vector<vector<int>> secondtable;
//                     while(rowsB.empty()==false && secondtable.size()!=pageB){
//                         secondtable.push_back(rowsB);
//                         rowsB=cursorB.getNext();
//                     }
//                     total_block_access+=1;
//                     for(auto a: temp_firsttable){
//                         for(auto b: secondtable){
//                             if(checkNestedConditionPart(a[attrA],b[attrB],parsedQuery.partHashBinaryOperation)){
//                                 vector<int> cur_temp;
//                                 cur_temp=a;
//                                 cur_temp.insert(cur_temp.end(),b.begin(),b.end());
//                                 resultTable->writeRow<int>(cur_temp);
//                             }
//                         }
//                     }
//                 }

//             }
//         }
//     }
//     return resultTable;
// }


// void executePARTHASHJOIN(){
//     logger.log("Execute part hash join");
//     Table tableA=*(tableCatalogue.getTable(parsedQuery.partHashJoinTableName1));
//     Table tableB=*(tableCatalogue.getTable(parsedQuery.partHashJoinTableName2));

//     Cursor cursorA=tableA.getCursor();
//     Cursor cursorB=tableB.getCursor();

//     int attrA=tableA.getColumnIndex(parsedQuery.partHashAttribute1);
//     int attrB=tableB.getColumnIndex(parsedQuery.partHashAttribute2);

//     int total_block_access=0;

//     int buffers=parsedQuery.bufferSize-1;

//     vector<string> tableAcolumns;
//     for(int i=0;i<tableA.columnCount;i++){
//         tableAcolumns.push_back(tableA.columns[i]);
//     }
//     vector<Table*> tableABuffers(buffers);
//     for(int i=0;i<buffers;i++){
//         tableABuffers[i]=new Table(tableA.tableName+"_"+to_string(i),tableAcolumns);
//     }
    
//     cursorA=tableA.getCursor();
//     vector<int>rowsA=cursorA.getNext();

//     while(rowsA.empty()==false){
//         int hash=rowsA[attrA]%buffers;
//         tableABuffers[hash]->writeRow<int>(rowsA);
//         rowsA=cursorA.getNext();
//     }
//     total_block_access+=tableA.blockCount;
//     for(int i=0;i<buffers;i++){
//         tableABuffers[i]->blockify();
//         total_block_access+=tableABuffers[i]->blockCount;
//         tableCatalogue.insertTable(tableABuffers[i]);
//     }

//     vector<string> tableBcolumns;
//     for(int i=0;i<tableB.columnCount;i++){
//         tableBcolumns.push_back(tableB.columns[i]);
//     }
//     vector<Table*> tableBBuffers(buffers);
//     for(int i=0;i<buffers;i++){
//         tableBBuffers[i]=new Table(tableB.tableName+"_"+to_string(i),tableBcolumns);
//     }
    
//     cursorB=tableB.getCursor();
//     vector<int>rowsB=cursorB.getNext();

//     while(rowsB.empty()==false){
//         int hash=rowsB[attrB]%buffers;
//         tableBBuffers[hash]->writeRow<int>(rowsB);
//         rowsB=cursorB.getNext();
//     }
//     total_block_access+=tableB.blockCount;
//     for(int i=0;i<buffers;i++){
//         tableBBuffers[i]->blockify();
//         total_block_access+=tableBBuffers[i]->blockCount;
//         tableCatalogue.insertTable(tableBBuffers[i]);
//     }



//     Table* resultTable = join(parsedQuery.bufferSize-1,total_block_access);
//     resultTable->blockify();
//     tableCatalogue.insertTable(resultTable);
//     for(int i=0;i<parsedQuery.bufferSize-1;i++){
//         tableCatalogue.deleteTable(parsedQuery.partHashJoinTableName1+"_"+to_string(i));
//         tableCatalogue.deleteTable(parsedQuery.partHashJoinTableName2+"_"+to_string(i));
//     }

//     cout<<"Total no of block accesses : "<<total_block_access+resultTable->blockCount<<endl;
//     return;

// }