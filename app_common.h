#pragma once
#include "data.h"
#include "schema.h"
#include "operator.h"

using namespace std;

static __thread bool init_properties=false;
static __thread bool generate_properties_file=false;
static properties prop_app;


static const int DEFAULT_SYNC_INTERVAL = 2;
static const int DEFAULT_HIST_START = 10;
static const int DEFAULT_HIST_STOP = 150;
static const int DEFAULT_HIST_COL_WIDTH = 10;
static const int DEFAULT_SOURCE_MIN = 10;
static const int DEFAULT_SOURCE_MAX = 150;
static const int DEFAULT_SOURCE_ITERATIONS = 5;
static const int DEFAULT_ITEMS_PER_RECORD = 10;

const string KEY_SYNC_INTERVAL = "sync.interval";
const string KEY_HIST_START = "histogram.start";
const string KEY_HIST_STOP = "histogram.end";
const string KEY_HIST_COL_WIDTH  = "histogram.col.width";
static const string KEY_SOURCE_MIN = "rnd.source.min";
static const string KEY_SOURCE_MAX = "rnd.source.max";
static const string KEY_SOURCE_ITERATIONS = "rnd.source.iters";
static const string KEY_ITEMS_PER_RECORD = "source.items.record";

propertiesPtr init_props(){

    if (generate_properties_file) {
        propertiesPtr props = boost::make_shared<properties>();
        //init enum strings

        map<string, string> pMap;
        pMap[KEY_SYNC_INTERVAL] = to_string(DEFAULT_SYNC_INTERVAL);
        pMap[KEY_HIST_START] = to_string(DEFAULT_HIST_START);
        pMap[KEY_HIST_STOP] = to_string(DEFAULT_HIST_STOP);
        pMap[KEY_HIST_COL_WIDTH] = to_string(DEFAULT_HIST_COL_WIDTH);
        pMap[KEY_SOURCE_MIN] = to_string(DEFAULT_SOURCE_MIN);
        pMap[KEY_SOURCE_MAX] = to_string(DEFAULT_SOURCE_MAX);
        pMap[KEY_SOURCE_ITERATIONS] = to_string(DEFAULT_SOURCE_ITERATIONS);
        pMap[KEY_ITEMS_PER_RECORD] = to_string(DEFAULT_ITEMS_PER_RECORD);

        props->add("App.properties", pMap);
        ofstream out("app.properties");


        properties config("ApplicationConfiguration");
        out << config.enterStr();

        out << props->tagStr();

        out << config.exitStr();

        return props;
    } else {
        cout << "[Reading application configuration from app.properties]" << endl ;
        FILE* opConfig = fopen("app.properties", "r");
        FILEStructureParser parser(opConfig, 10000);
        propertiesPtr allOpTags = parser.nextFull();
        return *allOpTags->getContents().begin();
    }

}

string get_property(string key){
    if(!init_properties){
        propertiesPtr props = init_props();
        init_properties = true ;
        assert(props.get() != NULL);
        prop_app = (*props.get());
        return props->begin().get(key);
    }
    return prop_app.begin().get(key);

}
