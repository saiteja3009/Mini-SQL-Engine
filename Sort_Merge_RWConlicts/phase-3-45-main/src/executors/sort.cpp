#include"global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    // logger.log("syntacticParseSORT");
    // if(tokenizedQuery.size()!= 8 || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN"){
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }
    // parsedQuery.queryType = SORT;
    // parsedQuery.sortResultRelationName = tokenizedQuery[0];
    // parsedQuery.sortRelationName = tokenizedQuery[3];
    // parsedQuery.sortColumnName = tokenizedQuery[5];
    // string sortingStrategy = tokenizedQuery[7];
    // if(sortingStrategy == "ASC")
    //     parsedQuery.sortingStrategy = ASC;
    // else if(sortingStrategy == "DESC")
    //     parsedQuery.sortingStrategy = DESC;
    // else{
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }
    // return true;
    logger.log("syntacticParseSORT");
    
    if ((tokenizedQuery.size() != 8 && tokenizedQuery.size()!=10) || tokenizedQuery[4] != "BY" || tokenizedQuery[6]!="IN" || tokenizedQuery[1]!="<-"){
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.nBuffers=10;
    if(tokenizedQuery.size()==10){
        if(tokenizedQuery[8] != "BUFFER" ){
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        string buf = tokenizedQuery[9];
        for (int i = 0; i < buf.length(); i++){
            if (!(buf[i]>='0' && buf[i]<='9')){
                cout<<"SYNTAX ERROR"<<endl;
                return false;
            }
        }
        parsedQuery.nBuffers = stoi(tokenizedQuery[9]);

    }
    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    
    
    string sortingStrategy = tokenizedQuery[7];
    if(sortingStrategy == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if(sortingStrategy == "DESC" || sortingStrategy == "DSC")
        parsedQuery.sortingStrategy = DESC;
    else{
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    return true;
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

    if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    return true;
}


// struct comp {
//     bool operator()(pair<int, int>& a, pair<int, int>& b) {
//         if (parsedQuery.sortingStrategy == ASC) {
//             return a.first > b.first;
//         }
//         return a.first < b.first;
//     }
// };

int column_index;
bool sortType=true;

struct comp {
    bool operator()(
        pair<int, int>& a, pair<int, int>& b) {
        if (parsedQuery.sortingStrategy == ASC) {
            return a.first > b.first;
        }
        return a.first < b.first;
    }
};


bool cmp(vector<int> &a,vector<int> &b){
    if (!sortType) {
        return a[column_index] < b[column_index];
    }
    return a[column_index] > b[column_index];

}



void executeSORT(){ 

    logger.log("executeSort");
    vector<Table*> tables;
    string final_table=parsedQuery.sortResultRelationName;
    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    int nBuffers = parsedQuery.nBuffers-1;
    column_index = table->getColumnIndex(parsedQuery.sortColumnName);
    // bool sortType=true;
    if(parsedQuery.sortingStrategy==ASC)
        sortType=false;
    int numberOfGroups = table->blockCount;
    Cursor cursor = table->getCursor();
    int block_count=table->blockCount;
    int i=0,j=0;


    while(i<block_count){
        vector<vector<int>> data;
        j=0;
        while(j<table->rowsPerBlockCount[i]){
            auto current_row=cursor.getNext();
            data.emplace_back(current_row);
            j++;
        }
        sort(data.begin(),data.end(),cmp);
        //for each page create tables
        Table* cur_table = new Table(final_table + "_cur_0_" + to_string(i));
        cur_table->columns = table->columns;
        cur_table->rowsPerBlockCount.push_back(data.size());
        cur_table->blockCount = 1;
        cur_table->columnCount = table->columnCount;
        cur_table->maxRowsPerBlock = table->maxRowsPerBlock;
        cur_table->rowCount = data.size();
        tableCatalogue.insertTable(cur_table);
        // bufferManager.writePage(cur_table->tableName)
        bufferManager.writePage(cur_table->tableName, 0, data, data.size());

        // for(int k=0;k<data.size();k++){
        //     cur_table->writeRow<int>(data[k]);
        // }
        // cur_table->blockify();
        tables.emplace_back(cur_table);
        i++;
    }
    i=1;
    while(true){
        int count = 0, tableCount = 0;
        vector<Table*> currIterationTables;
        int j=0;
        while(j<tables.size()){

            string curr_table_name = final_table+"_temp_"+to_string(i)+"_"+to_string(tableCount);
            if(numberOfGroups==1){
                curr_table_name = final_table;
            }
            Table *curr_table_obj = new Table(curr_table_name);
            curr_table_obj->columns = table->columns;
            curr_table_obj->maxRowsPerBlock = table->maxRowsPerBlock;
            curr_table_obj->columnCount = table->columnCount;
            tableCount++;

            vector<Cursor> cursors;
            int buff=nBuffers;
            
            while(buff-- && count<tables.size()){
                // Cursor cur(tables[count++]->tableName,0);
                // cursors.push_back(cur)
                cursors.push_back(tables[count++]->getCursor());
            }

            priority_queue<pair<int, int>, vector<pair<int, int>>, comp> pq;
            vector<vector<int>> cursorRows;
            
            for(int ind=0;ind<cursors.size();ind++){
                vector<int> v=cursors[ind].getNext();
                pq.push({v[column_index],ind});
                cursorRows.push_back(v);
            }
            
            vector<vector<int>> pq_data;
            while(pq.empty()==0){
                pair<int,int> p=pq.top();
                pq.pop();
                vector<int> current_row = cursorRows[p.second];
                pq_data.push_back(cursorRows[p.second]);

                if(pq_data.size()==table->maxRowsPerBlock){
                    bufferManager.writePage(curr_table_obj->tableName, curr_table_obj->blockCount,pq_data,pq_data.size());
                    curr_table_obj->blockCount++;
                    curr_table_obj->rowsPerBlockCount.push_back(pq_data.size());
                    curr_table_obj->rowCount+=pq_data.size();
                    pq_data.clear();
                }

                vector<int> next_row = cursors[p.second].getNext();
                if(next_row.size()>0){
                    cursorRows[p.second]=next_row;
                    pq.push({next_row[column_index],p.second});
                }
            }
            if(pq_data.empty()==0){
                bufferManager.writePage(curr_table_obj->tableName, curr_table_obj->blockCount,pq_data,pq_data.size());
                curr_table_obj->blockCount++;
                curr_table_obj->rowsPerBlockCount.push_back(pq_data.size());
                curr_table_obj->rowCount+=pq_data.size();
                pq_data.clear();
            }

            tableCatalogue.insertTable(curr_table_obj);
            currIterationTables.push_back(curr_table_obj);
            j+=nBuffers;
            
        }

        // for(int ind=0;ind<tables.size();ind++){
        //     tableCatalogue.deleteTable(tables[ind]->tableName);
        // }
        tables=currIterationTables;

        if(numberOfGroups<=1){
            break;
        }
        numberOfGroups=ceil(numberOfGroups*1.0/nBuffers);
        cout<<"Num of gros"<<numberOfGroups<<endl;
    }








    
}