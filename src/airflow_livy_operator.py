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
            https_conn,
            poll_interval=30,
            op_kwargs=None,
            *args, **kwargs):
        super(SparkLivyOperator, self).__init__(*args, **kwargs)
        self.user = user,
        self.password = password,
        self.https_conn = https_conn,
        self.session_kind = session_kind
        print(op_kwargs)
        self.payload = json.dumps(op_kwargs)

    def execute(self, context):
        print("Spark Livy Operator initiated.....")
        print("payload created is ....")
        print(self.payload)


class SparkLivyPlugin(AirflowPlugin):
    name = "spark_operator_plugin"
    operators = [SparkLivyOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
