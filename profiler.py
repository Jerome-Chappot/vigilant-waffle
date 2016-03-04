from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections
from collections import defaultdict
import urllib, json

connections.create_connection(hosts=['192.168.16.145']) # set the default connection

def pretty(d, indent=0):
    for key, value in d.items():
        print ('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent+1)
        else:
            print ('\t' * (indent+1) + str(value))

def avg_nb_con_per_request_per_clientip(): # return the average number of requests made by each client to each page
    avg_nb_con_per_request_per_clientip = {} # initiate the return dictionary
    s = Search(index="my_index")
    s.aggs.bucket('per_request', 'terms', field='request.untouched', size=0)\
        .bucket('per_clientip', 'terms', field='clientip', size=10000) # group results by URL, then by client IP
    response = s.extra(size=10000).execute() # execute the query
    for per_request in response.aggregations.per_request.buckets: # for each distinct URL
        nb_con_per_request = 0 # initiate the occurences counter
        for per_clientip in per_request.per_clientip.buckets: # for each distinct client IP
            nb_con_per_request += per_clientip.doc_count # count the occurrences
        avg_nb_con_per_request_per_clientip[per_request.key] = (nb_con_per_request / len(per_request.per_clientip.buckets)) # fill the dictionary with each different URL associated with its per client consultation average
    return avg_nb_con_per_request_per_clientip

def referrers_per_request(): # return the count of occurrences for each page/previous page couple
    referrers_per_request = {}
    s = Search(index="my_index")
    s.aggs.bucket('per_request', 'terms', field='request.untouched', size=0)\
        .bucket('per_referrer', 'terms', field='referrer.untouched', size=10000)
    response = s.extra(size=10000).execute()
    for per_request in response.aggregations.per_request.buckets: # for each distinct URL
        referrers_per_request[per_request.key] = {}
        for per_referrer in per_request.per_referrer.buckets: # for each distinct previous page
            referrers_per_request[per_request.key][per_referrer.key] = per_referrer.doc_count # fill the dictionary with each different URL associated with each different previous one with the number of occurrences for each of these relationships
    return referrers_per_request

def avg_bytes_per_request(): # return the average size of the object returned to the client for each URL
    avg_bytes_per_request = {}
    s = Search(index="my_index")
    s.aggs.bucket('per_request', 'terms', field='request.untouched', size=0)\
        .metric('avg_bytes', 'avg', field='bytes') # group) results by URL, then average the byte size
    response = s.extra(size=10000).execute()
    for per_request in response.aggregations.per_request.buckets:
        avg_bytes_per_request[per_request.key] = per_request.avg_bytes.value # fill the dictionary with each different URL associated with the average size of the object they return
    return avg_bytes_per_request

def parameters(): # return stats about parameter values
    parameters = defaultdict(dict)
    for hit in Search(index="my_index").fields("request").extra(size=10000).execute(): # starting the loop for indexing parameters in a multidimensional dictionary
        request = urllib.parse.unquote(hit.request[0])
        url = request.split('?')[0] # url == '/index.php'
        params = request.split('?')[1] if len(request.split('?')) == 2 else '' # params == route=total/shipping/country&country_id=21
        for param in params.split('&') if params != '' else {}: # for each parameter in ['route=total/shipping/country', 'country_id=21']
            param_name = param.split('=')[0] # param_name == route
            param_value = param.split('=')[1] if len(param.split('=')) == 2 else '' # param_value == total/shipping/country
            if url in parameters and param_name in parameters[url] and param_value in parameters[url][param_name]: # if the parameter value has already been indexed
                parameters[url][param_name][param_value] += 1
            else:
                if param_name not in parameters[url]:
                    parameters[url][param_name] = {}
                parameters[url][param_name][param_value] = 1
    for url in parameters: # starting the loop for couting letter occurrences for each parameter
        for param_name in parameters[url]: # for each parameter
            param_letters = param_count = 0
            parameters[url][param_name]['_letters_'] = {} # set an empty dictionary which will be filled with the letters used in the current parameter associated with their respective number of occurrences
            for param_value in parameters[url][param_name]: # for each value of the current parameter
                if not (param_value.startswith('_') and param_value.endswith('_')): # if the dictionary we are currently dealing with is the one of an actual parameter value and not a special one previously created for storing stats
                    param_value_occ_count = parameters[url][param_name][param_value] # number of occurrences of the parameter value
                    param_count += param_value_occ_count
                    for letter in param_value: # for each letter of the parameter value
                        param_letters += param_value_occ_count
                        if letter in parameters[url][param_name]['_letters_']: # if the current letter has already been indexed
                            parameters[url][param_name]['_letters_'][letter] += param_value_occ_count # increment the letter count by the number of occurrences of the parameter value
                        else:
                            parameters[url][param_name]['_letters_'][letter] = param_value_occ_count # do it by creating a new dictionary for the newly indexed letter
            parameters[url][param_name]['_letters_']['_avg_'] = param_letters / param_count
    return parameters


for name, content in {
    'avg_nb_con_per_request_per_clientip'   :avg_nb_con_per_request_per_clientip(),
    'referrers_per_request'                 :referrers_per_request(),
    'avg_bytes_per_request'                 :avg_bytes_per_request(),
    'parameters'                            :parameters()
}.items():
    f = open(name+'.json', 'w')
    print(json.dumps(content, indent = 4), file = f)
    f.close()
