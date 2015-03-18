#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_flow.h"
#include <iostream>
#include <stdio.h>

using namespace std;
typedef bool (*test_func)();

void testSuccess(string label);
void testSuccess();
void testFailure(string label);
void testFailure();

void registerTest(string test_name, test_func t);
void runTests(string label = "Default Flow Tests");



