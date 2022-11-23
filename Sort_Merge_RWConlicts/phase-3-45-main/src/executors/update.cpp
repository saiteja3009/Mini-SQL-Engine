#include "global.h"

bool syntacticParseUPDATE(){
    logger.log("syntacticParseUPDATE");
    if (tokenizedQuery.size() != 6) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery[0] != "UPDATE" || tokenizedQuery[2] != "COLUMN") {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    string str=tokenizedQuery.back();
    for(int i=0;i<str.size();i++){
        if(!(str[i]>='0' && str[i]<='9')){
            cout << "SYNTAX ERROR: The given update value is not an integer" << endl;
            return false;
        }
    }

    parsedQuery.queryType = UPDATE;
    parsedQuery.updateTable = tokenizedQuery[1];
    parsedQuery.updateColumn = tokenizedQuery[3];
    parsedQuery.updateOperator = tokenizedQuery[4];
    parsedQuery.updateVal = stoi(tokenizedQuery[5]);

    return true;


}


bool semanticParseUPDATE(){
    logger.log("semanticParseUPDATE");

    if (!tableCatalogue.isTable(parsedQuery.updateTable)) {
        cout << "SEMANTIC ERROR: Table not found" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.updateColumn, parsedQuery.updateTable)) {
        cout << "SEMANTIC ERROR: Column not found" << endl;
        return false;
    }

    return true;

}

void executeUPDATE() {
    logger.log("executeUPDATE");
    Table* table = tableCatalogue.getTable(parsedQuery.updateTable);
    bufferManager.clearPool();
    
    sleep(2);
    int value=0;
    while(true){
        cout << "Waiting for lock......" << endl;
        ifstream ip("../data/temp/"+table->tableName+"_lock",ios::in);
        ip>>value;
        ip.close();
        if(value==0){
            break;
        }
        sleep(3);
    }
    value--;

    ofstream op("../data/temp/"+table->tableName+"_lock",ios::trunc);
    op<<value<<endl;
    op.close();

    cout << "Acquiring lock......" << endl;
    Page page = bufferManager.getPage(parsedQuery.updateTable, 0);
    page.updatePage(parsedQuery.updateOperator,table->getColumnIndex(parsedQuery.updateColumn),parsedQuery.updateVal);

    
    sleep(2);

    ifstream if1("../data/temp/"+table->tableName+"_lock",ios::in);
    if1>>value;
    if1.close();
    value++;

    ofstream of1("../data/temp/"+table->tableName+"_lock",ios::trunc);
    of1<<value<<endl;
    of1.close();
    return ;
}