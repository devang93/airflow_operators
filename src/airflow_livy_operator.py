from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
import time, json, requests
# from airflow.utils import apply_defaults
from airflow.utils.decorators import apply_defaults


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
        self.user = user,
        self.password = password,
        self.session_kind = session_kind
        print(op_kwargs)
        self.livy_url = https_con
        self.payload = json.dumps(op_kwargs)

    def execute(self, context):
        print("Spark Livy Operator initiated.....")
        print("payload created is ....")
        print(self.payload)
        headers = {
            "content-type": "application/json",
            "cache-control": "no-cache"
        }
        try:
            response = requests.request("POST", url= self.https_con, auth=(self.user, self.password), data=self.payload, headers=headers)
            print(response.text)
            response_params = json.loads(response.text)
            print(response_params)
        except:
            print("Unable to submit spark livy job.")
            raise


class SparkLivyPlugin(AirflowPlugin):
    name = "spark_operator_plugin"
    operators = [SparkLivyOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
