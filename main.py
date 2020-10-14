import beam,flask,base64,json

def add_compute_engine_name(ele:dict):
    
        if 'environment' in ele:  
            if 'prod' in ele['environment']:
                ele['instance_name'] = f"gprod{ele['name']}"
                return ele
            if 'non-prod' in ele['environment']:
                ele['instance_name'] = f"gnprod{ele['name']}"
                return ele
            if 'staging' in ele['environment']:
                ele['instance_name'] = f"gstgn{ele['name']}"
                return ele


# def my_beam_func(request:flask.Request,context):

def my_beam_func(request:flask.Request):
    #pubsub_message = base64.b64decode(request['data']).decode('utf-8')
    pubsub_message = request.get_json(force=True)
    #data           = json.loads(pubsub_message)
    data           = add_compute_engine_name(pubsub_message)
    #data           = add_compute_engine_name(data)
    try:
        beam.filter_pipe(data)
    except:
        raise
    else:
        return {'status': "successfull"}
     
