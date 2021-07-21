#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

#include "qe_test_util.h"

//Parsing tuples -------------------------------------------------------------

void print_left_tuple(void *data) {
    cerr << setw(6) << *(int *)((char *)data + 0 + 1) << " | ";
    cerr << setw(6) << *(int *)((char *)data + 4 + 1) << " | ";
    cerr << setw(6) << fixed << setprecision(1) << *(float *)((char *)data + 8 + 1) << " | ";
}

void print_right_tuple(void *data) {
    cerr << setw(7) << *(int *)((char *)data + 0 + 1) << " | ";
    cerr << setw(7) << fixed << setprecision(1) << *(float *)((char *)data + 4 + 1) << " | ";
    cerr << setw(7) << *(int *)((float *)data + 8 + 1) << " | ";
}

//Query examples -------------------------------------------------------------

// SELECT * from left
void print_left() {
    TableScan *input = new TableScan(*rm, "left");

    // Go over the data through iterator
    void *data = malloc(bufSize);
    cerr << "==========================\n";
    cerr << "left.A | left.B | left.C | \n";

    while (input->getNextTuple(data) != QE_EOF) {
        //Print tuple
        print_left_tuple(data);

        cerr << endl;
        memset(data, 0, bufSize);
    }
}

// SELECT * from right
void print_right() {
    TableScan *input = new TableScan(*rm, "right");

    // Go over the data through iterator
    void *data = malloc(bufSize);
    cerr << "=============================\n";
    cerr << "right.B | right.C | right.D | \n";

    while (input->getNextTuple(data) != QE_EOF) {
        print_right_tuple(data);

        cerr << endl;
        memset(data, 0, bufSize);
    }
}

// SELECT * from left, right WHERE left.C = right.C
void print_join() {
    TableScan *leftIn = new TableScan(*rm, "left");
    IndexScan *rightIn = new IndexScan(*rm, "right", "C");

    // Create join condition
    Condition cond;
    cond.lhsAttr = "left.C";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.C";

    // Create INLJoin
    INLJoin *inlJoin = new INLJoin(leftIn, rightIn, cond);

    // Go over the data through iterator
    void *data = malloc(bufSize);
    cerr << "========================================================\n";
    cerr << "left.A | left.B | left.C | right.B | right.C | right.D | \n";

    while (inlJoin->getNextTuple(data) != QE_EOF) {
        print_left_tuple(data);
        print_right_tuple(data + 12);

        cerr << endl;
        memset(data, 0, bufSize);
    }
}

//Creating tables ------------------------------------------------------------

void setupTables() {
    rm->deleteCatalog();
    rm->createCatalog();

    createLeftTable();
    populateLeftTable();

    createRightTable();
    populateRightTable();
    createIndexforRightC();
}

//Create tables and demonstrate functionality using INLJoin
int main() {
    //Setup relations
    setupTables();

    //Perform queries
    cerr << "\nSELECT * from left:\n";
    print_left();  // SELECT * from left

    cerr << "\nSELECT * from right:\n";
    print_right();  // SELECT * from right

    cerr << "\nSELECT * from left, right WHERE left.C = right.C:\n";
    print_join();  // SELECT * from left, right WHERE left.C = right.C

    return EXIT_SUCCESS;
}
