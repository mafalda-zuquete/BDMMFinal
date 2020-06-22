import pandas as pd
from pymongo import MongoClient
from backend.DB import eu
from backend.DB import db

########################################################################################################################
countries = ['NO', 'HR', 'HU', 'CH', 'CZ', 'RO', 'LV', 'GR', 'GB', 'SI', 'LT',
             'ES', 'FR', 'IE', 'SE', 'NL', 'PT', 'PL', 'DK', 'MK', 'DE', 'IT',
             'BG', 'CY', 'AT', 'LU', 'BE', 'FI', 'EE', 'SK', 'MT', 'LI', 'IS']


def ex0_cpv_example(bot_year=2008, top_year=2020):
    """
    Returns all contracts in given year 'YEAR' range and cap to 100000000 the 'VALUE_EURO'

    Expected Output (list of documents):
    [{'result': count_value(int)}]
    """

    def year_filter(bot_year, top_year):
        filter_ = {
            '$match': {
                '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                'VALUE_EURO': {'$lt': 100000000}
            }}

        return filter_

    count = {
        '$count': 'result'
    }

    pipeline = [year_filter(bot_year, top_year), count]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents

def ex1_cpv_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns five metrics, described below
    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_cpv_euro_avg, avg_cpv_count, avg_cpv_offer_avg, avg_cpv_euro_avg_y_eu, avg_cpv_euro_avg_n_eu)

    Where:
    avg_cpv_euro_avg = average value of each CPV's division contracts average 'VALUE_EURO', (int)
    avg_cpv_count = average value of each CPV's division contract count, (int)
    avg_cpv_offer_avg = average value of each CPV's division contracts average NUMBER_OFFERS', (int)
    avg_cpv_euro_avg_y_eu = average value of each CPV's division contracts average VALUE_EURO' with 'B_EU_FUNDS', (int)
    avg_cpv_euro_avg_n_eu = average value of each CPV's division contracts average 'VALUE_EURO' with out 'B_EU_FUNDS' (int)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV': {'$ne': None}
        }
    }

    group_cpv_euro_avg = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    group_cpv_euro_avg_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_CPV_VALUE':{'$avg':'$AVERAGE_VALUE'}
        }
    }

    pipeline_cpv_euro_avg = [match, group_cpv_euro_avg, group_cpv_euro_avg_2]

    agg_cpv_euro_avg = list(eu.aggregate(pipeline_cpv_euro_avg))

    group_cpv_count = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'COUNT':{'$sum':1}
        }
    }

    group_cpv_count_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_COUNT':{'$avg':'$COUNT'}
        }
    }

    pipeline_cpv_count = [match, group_cpv_count, group_cpv_count_2]

    agg_cpv_count = list(eu.aggregate(pipeline_cpv_count))

    group_cpv_offer_avg = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVERAGE_OFFERS':{'$avg':'$NUMBER_OFFERS'}
        }
    }

    group_cpv_offer_avg_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_CPV_OFFERS':{'$avg':'$AVERAGE_OFFERS'}
        }
    }

    pipeline_cpv_offer_avg = [match, group_cpv_offer_avg, group_cpv_offer_avg_2]

    agg_cpv_offer_avg = list(eu.aggregate(pipeline_cpv_offer_avg))

    match_cpv_euro_avg_y_eu = {
        '$match':{
            'B_EU_FUNDS':'Y'
        }
    }

    pipeline_cpv_euro_avg_y_eu = [match, match_cpv_euro_avg_y_eu, group_cpv_euro_avg, group_cpv_euro_avg_2]

    agg_cpv_euro_avg_y_eu = list(eu.aggregate(pipeline_cpv_euro_avg_y_eu))

    match_cpv_euro_avg_n_eu = {
        '$match':{
            'B_EU_FUNDS':'N'
        }
    }

    pipeline_cpv_euro_avg_n_eu = [match, match_cpv_euro_avg_n_eu, group_cpv_euro_avg, group_cpv_euro_avg_2]

    agg_cpv_euro_avg_n_eu = list(eu.aggregate(pipeline_cpv_euro_avg_n_eu))

    avg_cpv_euro_avg = int(agg_cpv_euro_avg[0]['AVERAGE_CPV_VALUE'])
    avg_cpv_count = int(agg_cpv_count[0]['AVERAGE_COUNT'])
    avg_cpv_offer_avg = int(agg_cpv_offer_avg[0]['AVERAGE_CPV_OFFERS'])
    avg_cpv_euro_avg_y_eu = int(agg_cpv_euro_avg_y_eu[0]['AVERAGE_CPV_VALUE'])
    avg_cpv_euro_avg_n_eu = int(agg_cpv_euro_avg_n_eu[0]['AVERAGE_CPV_VALUE'])

    return avg_cpv_euro_avg, avg_cpv_count, avg_cpv_offer_avg, avg_cpv_euro_avg_y_eu, avg_cpv_euro_avg_n_eu


def ex2_cpv_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the count of contracts for each CPV Division
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, count: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = contract count of each CPV Division, (int)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV': {'$ne': None}
        }
    }

    group_cpv_count = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'COUNT':{'$sum':1}
        }
    }

    lookup = {
        '$lookup':{
            'from':'cpv',
            'localField':'_id',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'count':'$COUNT'
        }
    }

    project_2 = {
        '$project':{
            'cpv':'$cpv.cpv_division_description',
            'count':True
        }
    }

    pipeline = [match, group_cpv_count, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex3_cpv_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the highest 5 cpvs
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV': {'$ne': None}
        }
    }

    group_cpv_euro_avg = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE_VALUE':-1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'cpv',
            'localField':'_id',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'avg':'$AVERAGE_VALUE'
        }
    }

    project_2 = {
        '$project':{
            'cpv':'$cpv.cpv_division_description',
            'avg':True
        }
    }

    pipeline = [match, group_cpv_euro_avg, sort, limit, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex4_cpv_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the lowest 5 cpvs
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV': {'$ne': None}
        }
    }

    group_cpv_euro_avg = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE_VALUE':1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'cpv',
            'localField':'_id',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'avg':'$AVERAGE_VALUE'
        }
    }

    project_2 = {
        '$project':{
            'cpv':'$cpv.cpv_division_description',
            'avg':True
        }
    }

    pipeline = [match, group_cpv_euro_avg, sort, limit, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex5_cpv_bar_3(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the highest 5 cpvs for contracts which recieved european funds ('B_EU_FUNDS') 
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'B_EU_FUNDS':'Y',
            'CPV': {'$ne': None}
        }
    }

    group_cpv_euro_avg = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE_VALUE':-1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'cpv',
            'localField':'_id',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'avg':'$AVERAGE_VALUE'
        }
    }

    project_2 = {
        '$project':{
            'cpv':'$cpv.cpv_division_description',
            'avg':True
        }
    }

    pipeline = [match, group_cpv_euro_avg, sort, limit, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex6_cpv_bar_4(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the highest 5 cpvs for contracts which did not recieve european funds ('B_EU_FUNDS') 
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """
    
    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'B_EU_FUNDS':'N',
            'CPV': {'$ne': None}
        }
    }

    group_cpv_euro_avg = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE_VALUE':-1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'cpv',
            'localField':'_id',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'avg':'$AVERAGE_VALUE'
        }
    }

    project_2 = {
        '$project':{
            'cpv':'$cpv.cpv_division_description',
            'avg':True
        }
    }

    pipeline = [match, group_cpv_euro_avg, sort, limit, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex7_cpv_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the highest CPV Division on average 'VALUE_EURO' per country 'ISO_COUNTRY_CODE'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, avg: value_2, country: value_3}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = highest CPV Division average 'VALUE_EURO' of country, (float)
    value_3 = country in ISO-A2 format (string) (located in iso_codes collection)

    When choosing between using the country names or the ISO-3 codes, it was decided to used the ISO-3
    codes
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV': {'$ne': None}
        }
    }

    group_cpv_country_euro_avg = {
        '$group':{
            '_id':{'CPV_DIVISION':'$CPV_DIVISION','COUNTRY':'$ISO_COUNTRY_CODE'},
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE_VALUE':-1
        }
    }

    group_2 = {
        '$group':{
            '_id':'$_id.COUNTRY',
            'VALUE':{'$first':'$AVERAGE_VALUE'},
            'CPV':{'$first':'$_id.CPV_DIVISION'}
        }
    }

    lookup_country = {
        '$lookup':{
            'from':'iso_codes',
            'localField':'_id',
            'foreignField':'alpha-2',
            'as':'COUNTRY'
        }
    }

    lookup_cpv = {
        '$lookup':{
            'from':'cpv',
            'localField':'CPV',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'country':{ "$arrayElemAt": [ "$COUNTRY", 0] },
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'avg':'$VALUE'
        }
    }

    project_2 = {
        '$project':{
            'country':'$country.alpha-3',
            'cpv':'$cpv.cpv_division_description',
            'avg':True
        }
    }

    pipeline = [match, group_cpv_country_euro_avg, sort, group_2, lookup_country, lookup_cpv, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex8_cpv_hist(bot_year=2008, top_year=2020, country_list=countries, cpv='50'):
    """
    Produce an histogram where each bucket has the contract counts of a particular cpv
     in a given range of values (bucket) according to 'VALUE_EURO'

     Choose 10 buckets of any partition
    Buckets Example:
     0 to 100000
     100000 to 200000
     200000 to 300000
     300000 to 400000
     400000 to 500000
     500000 to 600000
     600000 to 700000
     700000 to 800000
     800000 to 900000
     900000 to 1000000


    So given a CPV Division code (two digit string) return a list of documents where each document as the bucket _id,
    and respective bucket count.

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{bucket: value_1, count: value_2}, ....]

    Where:
    value_1 = lower limit of respective bucket (if bucket position 0 of example then bucket:0 )
    value_2 = contract count for thar particular bucket, (int)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV_DIVISION': {'$eq': cpv}
        }
    }

    bucket = {
        '$bucket':{
            'groupBy':'$VALUE_EURO',
            'boundaries':[0,100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000],
            'default':1000000,
            'output':{
                'count':{'$sum':1}
            }
        }
    }

    project = {
        '$project':{
            '_id':False,
            'bucket':'$_id',
            'count':True
        }
    }

    pipeline = [match, bucket, project]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents

def ex9_cpv_bar_diff(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average time and value difference for each CPV, return the highest 5 cpvs

    time difference = 'DT-DISPATCH' - 'DT-AWARD'
    value difference = 'AWARD_VALUE_EURO' - 'VALUE_EURO'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, time_difference: value_2, value_difference: value_3}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'DT-DISPACH' - 'DT-AWARD', (float)
    value_3 = average 'EURO_AWARD' - 'VALUE_EURO' (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CPV': {'$ne': None},
            'AWARD_VALUE_EURO': {'$exists': True},
            'DT_DISPATCH': {'$ne': None},
            'DT_AWARD': {'$ne': None}
        }
    }

    project = {
        '$project':{
            '_id':False,
            'CPV_DIVISION':True,
            'TIME_DIFFERENCE':{'$subtract':['$DT_DISPATCH','$DT_AWARD']},
            'VALUE_DIFFERENCE':{'$subtract':['$AWARD_VALUE_EURO','$VALUE_EURO']}
        }
    }

    group_cpv_difference = {
        '$group':{
            '_id':'$CPV_DIVISION',
            'AVG_TIME_DIFFERENCE':{'$avg':'$TIME_DIFFERENCE'},
            'AVG_VALUE_DIFFERENCE':{'$avg':'$VALUE_DIFFERENCE'}
        }
    }

    sort = {
        '$sort':{
            'AVG_TIME_DIFFERENCE':-1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'cpv',
            'localField':'_id',
            'foreignField':'cpv_division',
            'as':'CPV'
        }
    }

    project_2 = {
        '$project':{
            '_id':False,
            'cpv':{ "$arrayElemAt": [ "$CPV", 0] },
            'time_difference':'$AVG_TIME_DIFFERENCE',
            'value_difference':'$AVG_VALUE_DIFFERENCE'
        }
    }

    project_3 = {
        '$project':{
            'cpv':'$cpv.cpv_division_description',
            'time_difference':True,
            'value_difference':True
        }
    }

    pipeline = [match, project, group_cpv_difference, sort, limit, lookup, project_2, project_3]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex10_country_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want five numbers, described below
    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_country_euro_avg, avg_country_count, avg_country_offer_avg, avg_country_euro_avg_y_eu, avg_country_euro_avg_n_eu)

    Where:
    avg_country_euro_avg = average value of each countries ('ISO_COUNTRY_CODE') contracts average 'VALUE_EURO', (int)
    avg_country_count = average value of each countries ('ISO_COUNTRY_CODE') contract count, (int)
    avg_country_offer_avg = average value of each countries ('ISO_COUNTRY_CODE') contracts average NUMBER_OFFERS', (int)
    avg_country_euro_avg_y_eu = average value of each countries ('ISO_COUNTRY_CODE') contracts average VALUE_EURO' with 'B_EU_FUNDS', (int)
    avg_country_euro_avg_n_eu = average value of each countries ('ISO_COUNTRY_CODE') contracts average 'VALUE_EURO' with out 'B_EU_FUNDS' (int)
    """
    
    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list}
        }
    }

    group_country_euro_avg = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    group_country_euro_avg_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_COUNTRY_VALUE':{'$avg':'$AVERAGE_VALUE'}
        }
    }

    pipeline_country_euro_avg = [match, group_country_euro_avg, group_country_euro_avg_2]

    agg_country_euro_avg = list(eu.aggregate(pipeline_country_euro_avg))

    group_country_count = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'COUNT':{'$sum':1}
        }
    }

    group_country_count_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_COUNT':{'$avg':'$COUNT'}
        }
    }

    pipeline_country_count = [match, group_country_count, group_country_count_2]

    agg_country_count = list(eu.aggregate(pipeline_country_count))

    group_country_offer_avg = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'AVERAGE_OFFERS':{'$avg':'$NUMBER_OFFERS'}
        }
    }

    group_country_offer_avg_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_COUNTRY_OFFERS':{'$avg':'$AVERAGE_OFFERS'}
        }
    }

    pipeline_country_offer_avg = [match, group_country_offer_avg, group_country_offer_avg_2]

    agg_country_offer_avg = list(eu.aggregate(pipeline_country_offer_avg))

    match_country_euro_avg_y_eu = {
        '$match':{
            'B_EU_FUNDS':'Y'
        }
    }

    pipeline_country_euro_avg_y_eu = [match, match_country_euro_avg_y_eu, group_country_euro_avg, group_country_euro_avg_2]

    agg_country_euro_avg_y_eu = list(eu.aggregate(pipeline_country_euro_avg_y_eu))

    match_country_euro_avg_n_eu = {
        '$match':{
            'B_EU_FUNDS':'N'
        }
    }

    pipeline_country_euro_avg_n_eu = [match, match_country_euro_avg_n_eu, group_country_euro_avg, group_country_euro_avg_2]

    agg_country_euro_avg_n_eu = list(eu.aggregate(pipeline_country_euro_avg_n_eu))

    avg_country_euro_avg = int(agg_country_euro_avg[0]['AVERAGE_COUNTRY_VALUE'])
    avg_country_count = int(agg_country_count[0]['AVERAGE_COUNT'])
    avg_country_offer_avg = int(agg_country_offer_avg[0]['AVERAGE_COUNTRY_OFFERS'])
    avg_country_euro_avg_y_eu = int(agg_country_euro_avg_y_eu[0]['AVERAGE_COUNTRY_VALUE'])
    avg_country_euro_avg_n_eu = int(agg_country_euro_avg_n_eu[0]['AVERAGE_COUNTRY_VALUE'])

    return avg_country_euro_avg, avg_country_count, avg_country_offer_avg, avg_country_euro_avg_y_eu, avg_country_euro_avg_n_eu


def ex11_country_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the count of contracts per country ('ISO_COUNTRY_CODE')
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{country: value_1, count: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in iso_codes collection')
    value_2 = contract count of each country, (int)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list}
        }
    }

    group_country_count = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'COUNT':{'$sum':1}
        }
    }

    lookup = {
        '$lookup':{
            'from':'iso_codes',
            'localField':'_id',
            'foreignField':'alpha-2',
            'as':'COUNTRY'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'country':{ "$arrayElemAt": [ "$COUNTRY", 0] },
            'count':'$COUNT'
        }
    }

    project_2 = {
        '$project':{
            'country':'$country.name',
            'count':True
        }
    }

    pipeline = [match, group_country_count, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex12_country_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'VALUE_EURO' for each country, return the highest 5 countries

    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{country: value_1, avg: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each country ('ISO_COUNTRY_CODE') name, (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list}
        }
    }

    group = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'AVERAGE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE': -1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'iso_codes',
            'localField':'_id',
            'foreignField':'alpha-2',
            'as':'COUNTRY'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'country':{ "$arrayElemAt": [ "$COUNTRY", 0] },
            'avg':'$AVERAGE'
        }
    }

    project_2 = {
        '$project':{
            'country':'$country.name',
            'avg':True
        }
    }

    pipeline = [match, group, sort, limit, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex13_country_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Group by country and get the average 'VALUE_EURO' for each group, return the lowest, average wise, 5 documents

    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{country: value_1, avg: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each country ('ISO_COUNTRY_CODE') name, (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list}
        }
    }

    group = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'AVERAGE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE': 1
        }
    }

    limit = {
        '$limit':5
    }

    lookup = {
        '$lookup':{
            'from':'iso_codes',
            'localField':'_id',
            'foreignField':'alpha-2',
            'as':'COUNTRY'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'country':{ "$arrayElemAt": [ "$COUNTRY", 0] },
            'avg':'$AVERAGE'
        }
    }

    project_2 = {
        '$project':{
            'country':'$country.name',
            'avg':True
        }
    }

    pipeline = [match, group, sort, limit, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex14_country_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    For each country get the sum of the respective contracts 'VALUE_EURO' with 'B_EU_FUNDS'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{sum: value_1, country: value_2}, ....]

    Where:
    value_1 = sum 'VALUE_EURO' of country ('ISO_COUNTRY_CODE') name, (float)
    value_2 = country in ISO-A2 format (string) (located in iso_codes collection)

    When choosing between using the country names or the ISO-3 codes, it was decided to used the ISO-3
    codes
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'B_EU_FUNDS': 'Y'
        }
    }

    group = {
        '$group':{
            '_id':'$ISO_COUNTRY_CODE',
            'SUM':{'$sum':'$VALUE_EURO'}
        }
    }

    lookup = {
        '$lookup':{
            'from':'iso_codes',
            'localField':'_id',
            'foreignField':'alpha-2',
            'as':'COUNTRY'
        }
    }

    project = {
        '$project':{
            '_id':False,
            'country':{ "$arrayElemAt": [ "$COUNTRY", 0] },
            'sum':'$SUM'
        }
    }

    project_2 = {
        '$project':{
            'country':'$country.alpha-3',
            'sum':True
        }
    }

    pipeline = [match, group, lookup, project, project_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex15_business_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want five numbers, described below

    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_business_euro_avg, avg_business_count, avg_business_offer_avg, avg_business_euro_avg_y_eu, avg_business_euro_avg_n_eu)

    Where:
    avg_business_euro_avg = average value of each company ('CAE_NAME')  contracts average 'VALUE_EURO', (int)
    avg_business_count = average value of each company ('CAE_NAME') contract count, (int)
    avg_business_offer_avg = average value of each company ('CAE_NAME') contracts average NUMBER_OFFERS', (int)
    avg_business_euro_avg_y_eu = average value of each company ('CAE_NAME') contracts average VALUE_EURO' with 'B_EU_FUNDS', (int)
    avg_business_euro_avg_n_eu = average value of each company ('CAE_NAME') contracts average 'VALUE_EURO' with out 'B_EU_FUNDS' (int)
    """
    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CAE_NAME': {'$exists': True}
        }
    }

    group_cae_euro_avg = {
        '$group':{
            '_id':'$CAE_NAME',
            'AVERAGE_VALUE':{'$avg':'$VALUE_EURO'}
        }
    }

    group_cae_euro_avg_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_CAE_VALUE':{'$avg':'$AVERAGE_VALUE'}
        }
    }

    pipeline_cae_euro_avg = [match, group_cae_euro_avg, group_cae_euro_avg_2]

    agg_business_euro_avg = list(eu.aggregate(pipeline_cae_euro_avg))
    
    group_cae_count = {
        '$group':{
            '_id':'$CAE_NAME',
            'COUNT':{'$sum':1}
        }
    }

    group_cae_count_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_COUNT':{'$avg':'$COUNT'}
        }
    }

    pipeline_cae_count = [match, group_cae_count, group_cae_count_2]

    agg_business_count = list(eu.aggregate(pipeline_cae_count))    

    group_cae_offer_avg = {
        '$group':{
            '_id':'$CAE_NAME',
            'AVERAGE_OFFERS':{'$avg':'$NUMBER_OFFERS'}
        }
    }

    group_cae_offer_avg_2 = {
        '$group':{
            '_id':None,
            'AVERAGE_CAE_OFFERS':{'$avg':'$AVERAGE_OFFERS'}
        }
    }

    pipeline_cae_offer_avg = [match, group_cae_offer_avg, group_cae_offer_avg_2]

    agg_business_offer_avg = list(eu.aggregate(pipeline_cae_offer_avg))

    match_cae_euro_avg_y_eu = {
        '$match':{
            'B_EU_FUNDS':'Y'
        }
    }

    pipeline_cae_euro_avg_y_eu = [match, match_cae_euro_avg_y_eu, group_cae_euro_avg, group_cae_euro_avg_2]

    agg_business_euro_avg_y_eu = list(eu.aggregate(pipeline_cae_euro_avg_y_eu))

    match_cae_euro_avg_n_eu = {
        '$match':{
            'B_EU_FUNDS':'N'
        }
    }

    pipeline_cae_euro_avg_n_eu = [match, match_cae_euro_avg_n_eu, group_cae_euro_avg, group_cae_euro_avg_2]

    agg_business_euro_avg_n_eu = list(eu.aggregate(pipeline_cae_euro_avg_n_eu))

    avg_business_euro_avg = int(agg_business_euro_avg[0]['AVERAGE_CAE_VALUE'])
    avg_business_count = int(agg_business_count[0]['AVERAGE_COUNT'])
    avg_business_offer_avg = int(agg_business_offer_avg[0]['AVERAGE_CAE_OFFERS'])
    avg_business_euro_avg_y_eu = int(agg_business_euro_avg_y_eu[0]['AVERAGE_CAE_VALUE'])
    avg_business_euro_avg_n_eu = int(agg_business_euro_avg_n_eu[0]['AVERAGE_CAE_VALUE'])

    return avg_business_euro_avg, avg_business_count, avg_business_offer_avg, avg_business_euro_avg_y_eu, avg_business_euro_avg_n_eu


def ex16_business_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'VALUE_EURO' for company ('CAE_NAME') return the highest 5 companies
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{company: value_1, avg: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') name, (string)
    value_2 = average 'VALUE_EURO' of each company ('CAE_NAME'), (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CAE_NAME': {'$exists': True}
        }
    }

    group = {
        '$group':{
            '_id':'$CAE_NAME',
            'AVERAGE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE': -1
        }
    }

    limit = {
        '$limit':5
    }


    project = {
        '$project':{
            '_id':False,
            'company':'$_id',
            'avg':'$AVERAGE'
        }
    }

    pipeline = [match, group, sort, limit, project]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex17_business_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'VALUE_EURO' for company ('CAE_NAME') return the lowest 5 companies


    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{company: value_1, avg: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') name, (string)
    value_2 = average 'VALUE_EURO' of each company ('CAE_NAME'), (float)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CAE_NAME': {'$exists': True}
        }
    }

    group = {
        '$group':{
            '_id':'$CAE_NAME',
            'AVERAGE':{'$avg':'$VALUE_EURO'}
        }
    }

    sort = {
        '$sort':{
            'AVERAGE': 1
        }
    }

    limit = {
        '$limit':5
    }


    project = {
        '$project':{
            '_id':False,
            'company':'$_id',
            'avg':'$AVERAGE'
        }
    }

    pipeline = [match, group, sort, limit, project]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex18_business_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want the count of contracts for each company 'CAE_NAME', for the highest 15
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{company: value_1, count: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME'), (string)
    value_2 = contract count of each company ('CAE_NAME'), (int)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CAE_NAME': {'$exists': True}
        }
    }

    group = {
        '$group':{
            '_id':'$CAE_NAME',
            'COUNT':{'$sum':1}
        }
    }

    sort = {
        '$sort':{
            'COUNT': -1
        }
    }

    limit = {
        '$limit':15
    }


    project = {
        '$project':{
            '_id':False,
            'company':'$_id',
            'count':'$COUNT'
        }
    }

    pipeline = [match, group, sort, limit, project]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex19_business_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    For each country get the highest company ('CAE_NAME') in terms of 'VALUE_EURO' sum contract spending

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{company: value_1, sum: value_2, country: value_3, address: value_4}, ....]

    Where:
    value_1 = 'top' company of that particular country ('CAE_NAME'), (string)
    value_2 = sum 'VALUE_EURO' of country and company ('CAE_NAME'), (float)
    value_3 = country in ISO-A2 format (string) (located in iso_codes collection)
    value_4 = company ('CAE_NAME') address, single string merging 'CAE_ADDRESS' and 'CAE_TOWN' separated by ' ' (space)

    When choosing between using the country names or the ISO-3 codes, it was decided to used the ISO-3
    codes
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CAE_NAME': {'$exists': True}
        }
    }

    group_cae_country_euro_avg = {
        '$group':{
            '_id':{'CAE_NAME':'$CAE_NAME','COUNTRY':'$ISO_COUNTRY_CODE'},
            'SUM':{'$sum':'$VALUE_EURO'},
            'ADDRESS':{'$first':'$ADDRESS'}        
        }
    }

    sort = {
        '$sort':{
            'SUM':-1
        }
    }

    group_2 = {
        '$group':{
            '_id':'$_id.COUNTRY',
            'VALUE':{'$first':'$SUM'},
            'COMPANY':{'$first':'$_id.CAE_NAME'},
            'ADDRESS':{'$first':'$ADDRESS'}
        }
    }

    lookup = {
        '$lookup':{
            'from':'iso_codes',
            'localField':'_id',
            'foreignField':'alpha-2',
            'as':'COUNTRY'
        }
    }

    project_2 = {
        '$project':{
            '_id':False,
            'country':{ "$arrayElemAt": [ "$COUNTRY", 0] },
            'company':'$COMPANY',
            'sum':'$VALUE',
            'address':'$ADDRESS'
        }
    }

    project_3 = {
        '$project':{
            'country':'$country.alpha-3',
            'company':True,
            'sum':True,
            'address':True
        }
    }

    pipeline = [match, group_cae_country_euro_avg, sort, group_2, lookup, project_2, project_3]

    list_documents = list(eu.aggregate(pipeline,allowDiskUse=True))

    return list_documents


def ex20_business_connection(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want the top 5 most frequent co-occurring companies ('CAE_NAME' and 'WIN_NAME')

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{companies: value_1, count: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') string merged with company ('WIN_NAME') seperated by the string ' with ', (string)
    value_2 = co-occurring number of contracts (int)
    """

    match = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
            'ISO_COUNTRY_CODE': {'$in': country_list},
            'CAE_NAME': {'$exists': True},
            'WIN_NAME': {'$exists': True},
        }
    }

    group = {
        '$group':{
            '_id':'$PAIR',
            'COUNT':{'$sum':1}
        }
    }

    sort = {
        '$sort':{
            'COUNT':-1
        }
    }

    limit = {
        '$limit':5
    }

    project_2 = {
        '$project':{
            '_id':False,
            'companies':'$_id',
            'count':'$COUNT'
        }
    }

    pipeline = [match, group, sort, limit, project_2]

    list_documents = list(eu.aggregate(pipeline,allowDiskUse=True))

    return list_documents

def insert_operation(document):
    '''
        Insert operation.

        In case pre computed tables were generated for the queries they should be recomputed with the new data.
    '''

    countries = ['NO', 'HR', 'HU', 'CH', 'CZ', 'RO', 'LV', 'GR', 'GB', 'SI', 'LT',
                 'ES', 'FR', 'IE', 'SE', 'NL', 'PT', 'PL', 'DK', 'MK', 'DE', 'IT',
                 'BG', 'CY', 'AT', 'LU', 'BE', 'FI', 'EE', 'SK', 'MT', 'LI', 'IS', 'UK']

    insert = db.new.insert_many(document).inserted_ids

    match = {
        '$match':{
            '$and':[{'YEAR':{'$gte':2008}},{'YEAR':{'$lte':2020}}],
            'VALUE_EURO':{'$lt':100000000},
            'ISO_COUNTRY_CODE':{'$in':countries},
            '_id':{'$in':insert}
        }
    }

    project = {
        '$project':{
            '_id':False,
            'VALUE_EURO':True,
            'NUMBER_OFFERS':True,
            'nCPV':'$CPV',
            'CPV':{'$toString':'$CPV'},
            'ISO_COUNTRY_CODE':True,
            'YEAR':True,
            'AWARD_VALUE_EURO':True,
            'DT_DISPATCH':{'$dateFromString':{'dateString':'$DT_DISPATCH'}},
            'DT_AWARD':{'$dateFromString':{'dateString':'$DT_AWARD'}},
            'B_EU_FUNDS':True,
            'CAE_NAME':True,
            'CAE_ADDRESS':True,
            'CAE_TOWN':True,
            'WIN_NAME':True,
            'ADDRESS':{'$concat':[{'$toString':'$CAE_ADDRESS'},' ',{'$toString':'$CAE_TOWN'}]},
            'PAIR':{'$concat':[{'$toString':'$CAE_NAME'},' with ',{'$toString':'$WIN_NAME'}]}
        }
    }
    
    pipeline = [match,project]
    
    list_documents = list(db.new.aggregate(pipeline))
    inserted_ids = None

    if len(list_documents) > 0:
        insert_2 = db.new_2.insert_many(list_documents).inserted_ids

        db.new_2.update_many({'$expr':{'$lt':['$nCPV',10000000]},'_id':{'$in':insert_2}},[{'$set':{'CPV':{'$concat':['0','$CPV']}}}])
        db.new_2.update_many({'ISO_COUNTRY_CODE':'UK','_id':{'$in':insert_2}},[{'$set':{'ISO_COUNTRY_CODE':'GB'}}])

        match_2 = {
            '$match':{
                '_id':{'$in':insert_2}
            }
        }

        project_2 = {
            '$project':{
                '_id':False,
                'VALUE_EURO':True,
                'NUMBER_OFFERS':True,
                'nCPV':True,
                'CPV':True,
                'ISO_COUNTRY_CODE':True,
                'YEAR':True,
                'AWARD_VALUE_EURO':True,
                'DT_DISPATCH':True,
                'DT_AWARD':True,
                'B_EU_FUNDS':True,
                'CAE_NAME':True,
                'CAE_ADDRESS':True,
                'CAE_TOWN':True,
                'WIN_NAME':True,
                'ADDRESS':True,
                'PAIR':True,
                'CPV_DIVISION':{'$substr':['$CPV',0,2]}
            }
        }

        pipeline_2 = [match_2,project_2]
        
        list_documents_2 = list(db.new_2.aggregate(pipeline_2))

        inserted_ids = eu.insert_many(list_documents_2).inserted_ids

    return inserted_ids


query_list = [
    ex1_cpv_box, ex2_cpv_treemap, ex3_cpv_bar_1, ex4_cpv_bar_2,
    ex5_cpv_bar_3, ex6_cpv_bar_4, ex7_cpv_map, ex8_cpv_hist ,ex9_cpv_bar_diff,
    ex10_country_box, ex11_country_treemap, ex12_country_bar_1,
    ex13_country_bar_2, ex14_country_map, ex15_business_box,
    ex16_business_bar_1, ex17_business_bar_2, ex18_business_treemap,
    ex19_business_map, ex20_business_connection
]
