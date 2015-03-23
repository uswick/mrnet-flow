#include "flow_test.h"
#include "app_common.h"
#include <string>

using namespace std;

bool test_generate_propfile(){
    generate_properties_file = true;

    if (FILE *file = fopen("app.properties", "r")) {
        fclose(file);
    } else {
        testFailure();
    }
    return true;
}

bool test_default_value_with_gen(){
    generate_properties_file = true;

    if(get_property(KEY_HIST_COL_WIDTH) != to_string(DEFAULT_HIST_COL_WIDTH)){
        testFailure();
    }
    if(get_property(KEY_HIST_START) != to_string(DEFAULT_HIST_START)){
        testFailure();
    }
    if(get_property(KEY_HIST_STOP) != to_string(DEFAULT_HIST_STOP)){
        testFailure();
    }
    if(get_property(KEY_ITEMS_PER_RECORD) != to_string(DEFAULT_ITEMS_PER_RECORD)){
        testFailure();
    }
    if(get_property(KEY_SYNC_INTERVAL) != to_string(DEFAULT_SYNC_INTERVAL)){
        testFailure();
    }



    return true;
}

bool test_default_value_wih_prop_file(){
//    generate_properties_file = true;

    if(get_property(KEY_HIST_COL_WIDTH) != to_string(DEFAULT_HIST_COL_WIDTH)){
        testFailure();
    }
    if(get_property(KEY_HIST_START) != to_string(DEFAULT_HIST_START)){
        testFailure();
    }
    if(get_property(KEY_HIST_STOP) != to_string(DEFAULT_HIST_STOP)){
        testFailure();
    }
    if(get_property(KEY_ITEMS_PER_RECORD) != to_string(DEFAULT_ITEMS_PER_RECORD)){
        testFailure();
    }
    if(get_property(KEY_SYNC_INTERVAL) != to_string(DEFAULT_SYNC_INTERVAL)){
        testFailure();
    }



    return true;
}

int main(int argc, char** argv) {
    string test_suite = "operators::app_common_functionality";

    //register each inidividual test
    registerTest(test_suite + "::test_generate_propfile", &test_generate_propfile);
    registerTest(test_suite + "::test_default_value_with_gen", &test_default_value_with_gen);
    //registerTest(test_suite + "::test_default_value_wih_prop_file", &test_default_value_wih_prop_file);


    //run Tests which has been registered above
    runTests(test_suite);

    return 0;
}
