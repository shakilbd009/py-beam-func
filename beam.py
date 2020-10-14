import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os,json,firestore

def remove_unwanted_col(ele):
    new = ele.copy()
    del new['name']
    del new['project']
    del new['topic_name']
    del new['machine_type']
    new['cpu_type'] = ele['machine_type']
    return new

def add_other_col(ele):
    ele['deployment_status'] = 'pending'
    ele['job_status']        = 'incomplete'
    ele['instance_ip']       = 'pending'
    return ele

def filter_pipe(data:dict):
    pipe       = beam.Pipeline(options=PipelineOptions(streaming=True))
    project    = data['project']
    topic_name = data['topic_name']
    table      = os.getenv('bigquery_table')
    dataset    = os.getenv('bigquery_dataset')
    base       = (
        pipe 
        | 'create pcoll from dict' >> beam.Create([data])
    )

    write_to_compute_topic = (
        base
        | 'encode to byte string'  >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
        #| 'write to text' >> beam.io.WriteToText(topic_name.split('/')[-1])
        | 'write to pubsub topic'  >> beam.io.WriteToPubSub(topic=topic_name)
    )
    write_to_firestore = (
        base
        | 'add other colums'  >> beam.Map(add_other_col)
        | 'apply dofn'        >> beam.ParDo(firestore.FirestoreWriteDofn(project=project))
        | 'print'             >> beam.Map(print)
    )
    write_to_bigquery = (
        base
        | 'remove unwanted'   >> beam.Map(remove_unwanted_col)
        | 'add other colum'   >> beam.Map(add_other_col)
        | 'write to bigQuery' >> beam.io.WriteToBigQuery(
            table=table,
            dataset=dataset,
            project=project,
            schema='environment:STRING,zone:STRING,instance_name:STRING,deployment_status:STRING,job_status:STRING,cpu_type:STRING')
    )
    
    result = pipe.run()
    result.wait_until_finish()

# if __name__ == "__main__":
#     filter_pipe()
#     print(topic_name)