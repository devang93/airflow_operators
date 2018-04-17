from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from time import sleep
from airflow.utils.decorators import apply_defaults
import json, requests
from airflow.exceptions import AirflowException


class SparkLivyOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            user,
            password,
            session_kind,
            https_con,
            poll_interval=30,
            op_kwargs=None,
            *args, **kwargs):
        super(SparkLivyOperator, self).__init__(*args, **kwargs)
        self.user = user
        self.password = password
        self.session_kind = session_kind
        self.poll_interval = poll_interval
        self.livy_url = https_con
        self.payload = json.dumps(op_kwargs)

    def execute(self, context):
        print("Spark Livy Operator initiated.....")
        headers = {
            "content-type": "application/json",
            "cache-control": "no-cache"
        }
        try:
            response = requests.request("POST", url= self.livy_url, auth=(self.user, self.password), data=self.payload, headers=headers)
            response_params = json.loads(response.text)
            batch_id = response_params['id']
            print("Batch submitted to spark livy server: {}".format(batch_id))
            ret_code = self.__poll_spark_job(batch_id)
	    if ret_code == -1:
	        raise AirflowException("Apache Spark Livy Job failed!")
        except:
            print("Unable to submit spark livy job.")
            raise

    def __poll_spark_job(self, batch_id):
        try:
            state = "starting"
            while state != "dead" and state != "success":
                url = "{livy_url}{batch_id}".format(
                    livy_url=self.livy_url, batch_id=batch_id)
                headers = {
                    'content-type': "application/json",
                    'cache-control': "no-cache"
                }
                response = requests.request("GET", url, auth=(self.user, self.password), headers=headers)
                response_params = json.loads(response.text)
                state = response_params["state"]
                print("Batch {id} status: {state}".format(id=batch_id, state=state))
                if state != "success" and state != "dead":
                    sleep(self.poll_interval)
		else:
		    print("Job finished!")

            if state == "success":
                print("Spark Livy Batch ID {} run succeed.".format(batch_id))
                return 0
            else:
                print("Spark Livy Batch ID {} run failed.".format(batch_id))
                return -1
        except:
            print("Unable to get spark batch status for batch_id {}".format(batch_id))
            return -1

class SparkLivyPlugin(AirflowPlugin):
    name = "spark_operator_plugin"
    operators = [SparkLivyOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []