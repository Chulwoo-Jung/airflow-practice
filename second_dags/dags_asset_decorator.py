from airflow.sdk import asset, Asset, Context
import os

'''
1. When using @asset, dag and task are automatically generated
2. The name of the created Asset is set by the function name, not the uri value (my_produce_asset_1 or 2)
'''

@asset(
    dag_id='dags_asset_decorator_produce_1',
    schedule=None,
    tags=['asset'],
    uri='asset_by_decorator_1'
)
def my_produce_asset_1(context: Context):
    # You can perform various tasks that were done in the original task using the context variable (xcom usage, etc.)
    # It's recommended to specify the Context type through type hinting.
    from pprint import pprint
    pprint(context)
    ti = context.get('ti')
    ti.xcom_push(key="asset_name", value="my_produce_asset")


@asset(
    dag_id='dags_asset_decorator_produce_2',
    schedule=None,
    tags=['asset'],
    uri='asset_by_decorator_2'
)
def my_produce_asset_2(self, context: Context):
    # When accessing the Asset that the function will produce, you can use the self variable.
    file_path = f"/opt/airflow/dags"
    py_cnt = 0
    for (root, dir, file) in os.walk(file_path):
        for f in file:
            if f.endswith('.py'):
                py_cnt += 1
    context.get('outlet_events').get(self).extra = {"py_cnt": py_cnt}
    # You can specify the Asset that you yourself publish through self, but you can also find it by directly entering the name
    #context.get('outlet_events').get(Asset('my_produce_asset_2')).extra = {"py_cnt": py_cnt}

##########################################################################################################
# @asset can only be used to produce Assets (consume operations are not possible)
# However, you can reference other Asset information.
##########################################################################################################

consume_asset_2 = Asset('my_produce_asset_2')

@asset(
    dag_id='dags_asset_decorator_produce_n_refer',
    schedule=None
)
def produce_and_refer(self, context: Context, my_produce_asset_2: Asset):
    from pprint import pprint
    print("::group::print context information")
    pprint(context)
    print("::endgroup::")

    # Reference another Asset (my_produce_asset_2)
    extra_info = context['inlet_events'].get(my_produce_asset_2)[-1].extra
    print('my_produce_asset_2 extra information:', str(extra_info))