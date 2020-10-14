from google.cloud import firestore
import apache_beam as beam

class FirestoreWriteDofn(beam.DoFn):
    def __init__(self, project):
        self.project = project
    def process(self,ele):
        #db = firestore.Client(ele['project'])
        db = firestore.Client(self.project)
        data = {
            'instance_name': ele['instance_name'],
            'instance_ip': ele['instance_ip'],
            'job_status': ele['job_status'],
            'zone': ele['zone'],
            'deployment_status': ele['deployment_status']
        }
        fb = db.collection("my-compute-firestore-table").document(f"{ele['environment']}").collection("compute_engine").document(f"{ele['instance_name']}")
        result = fb.create(data)
        return [result]

class Testdofn(beam.DoFn):
    def process(self,ele):
        data = {
            'instance_name': ele['instance_name'],
            'instance_ip': ele['instance_ip'],
            'job_status': ele['job_status'],
            'zone': ele['zone'],
            'deployment_status': ele['deployment_status'],
            'test': 'test'
        }
        return [data]