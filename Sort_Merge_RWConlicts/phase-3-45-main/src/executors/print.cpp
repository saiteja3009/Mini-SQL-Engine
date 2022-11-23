#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT;
    parsedQuery.printRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINT() {
    logger.log("executePRINT");
    Table* table = tableCatalogue.getTable(parsedQuery.printRelationName);
    bufferManager.clearPool();

    sleep(2);

    // Read the lock filefrom the data
    int val = 0;

    while (true) {
        cout << "Acquiring lock......" << endl;
        ifstream ip("../data/temp/" + table->tableName + "_lock", ios::in);
        ip >> val;
        ip.close();
        if (val >= 0) {
            break;
        }
        sleep(5);
    }

    val += 1;
    ofstream op("../data/temp/" + table->tableName + "_lock", ios::trunc);
    op << val << endl;
    op.close();

    table->print();

    sleep(2);

    ifstream ipc("../data/temp/" + table->tableName + "_lock", ios::in);
    ipc >> val;
    ipc.close();
    val -= 1;

    ofstream opc("../data/temp/" + table->tableName + "_lock", ios::trunc);
    opc << val << endl;
    opc.close();

    return;
}
